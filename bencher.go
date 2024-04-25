package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/Azure/go-amqp"
)

type BencherFlags struct {
	verbose bool

	payloadSize  uint64
	payloadCount uint64
	goroutines   uint64
	maxInTransit uint64
	durable      bool

	receiverCode string
	messageType  string

	outboxSocketAddr string
	outboxAmqpUser   string
	outboxAmqpPass   string
	outboxReply      string
	outbox           string
	sendEvent        string

	inbox           string
	inboxSocketAddr string
	inboxAmqpUser   string
	inboxAmqpPass   string
}

type Bencher struct {
	BencherFlags

	outboxConnOpts *AMQPConnOpts
	inboxConnOpts  *AMQPConnOpts
	outboxConn     *AMQPConn
	inboxConn      *AMQPConn

	tracker        *Tracker
	messageTracker *MessageTracker
	queues         *Queues
}

func NewBencher() *Bencher {
	return &Bencher{}
}

// prepareOutbox connects the the sender endpoint broker and sets up senders for the outbox and
// receivers for the send event and outbox reply queues
func (bencher *Bencher) ConnectQueues() {
	var err error
	if bencher.inboxConnOpts != nil {
		bencher.inboxConnOpts.username = bencher.inboxAmqpUser
		bencher.inboxConnOpts.password = bencher.inboxAmqpPass

		bencher.inboxConn, err = newAMQPConn(bencher.inboxConnOpts)
		if err != nil {
			log.Fatal("Unable to create outbox AMQP client:", err)
		}
	}

	if bencher.outboxConnOpts != nil {
		bencher.outboxConnOpts.username = bencher.outboxAmqpUser
		bencher.outboxConnOpts.password = bencher.outboxAmqpPass

		bencher.outboxConn, err = newAMQPConn(bencher.outboxConnOpts)
		if err != nil {
			log.Fatal("Unable to create inbox AMQP client:", err)
		}
	}
}

func (b *Bencher) ConfigureTracker(trackerCount int, payloadSize uint64) {
	b.tracker = &Tracker{
		PayloadSize: int(payloadSize),
		Trackables:  make([]*Trackable, trackerCount),
	}
}

func (b *Bencher) CreateQueueEndpointOutbox(trackID int, isTracing bool, eventFlags int) *SenderQueue {
	sendTracker := NewTrackable()
	sendTracker.Name = "Sent to outbox"
	sendTracker.EstimatedEventSize = b.payloadSize
	sendTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID] = sendTracker

	queue := &SenderQueue{
		CreateMsg:         CreateMadesMsgCreator(b.receiverCode, b.messageType, isTracing, eventFlags, b.durable),
		PayloadSize:       b.payloadSize,
		Queue:             NewQueue(b.messageTracker, sendTracker),
		maxUnacknowledged: b.maxInTransit,
		sender:            b.outboxConn.senders[0],
		senderCount:       int(b.goroutines),
	}

	return queue
}

func (b *Bencher) CreateQueueEndpointOutboxReply(trackID int) *ReceiveQueue {
	replyTracker := NewTrackable()
	replyTracker.Name = "Sent to broker"
	replyTracker.EstimatedEventSize = b.payloadSize
	replyTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID] = replyTracker

	queue := &ReceiveQueue{
		IdentifyMsg: HandleMadesReplyMessage,
		Queue:       NewQueue(b.messageTracker, replyTracker),
		receivers: []*amqp.Receiver{
			b.outboxConn.receivers[0],
		},
	}

	return queue
}

func (b *Bencher) CreateQueueEndpointSendEvent(trackID int) *ReceiveQueue {
	deliveryTracker := NewTrackable()
	deliveryTracker.Name = "Delivery event"
	deliveryTracker.EstimatedEventSize = 100 // Roughly
	deliveryTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID] = deliveryTracker

	receivedTracker := NewTrackable()
	receivedTracker.Name = "Received event"
	receivedTracker.EstimatedEventSize = 100 // Roughly
	receivedTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID+1] = receivedTracker

	queue := &ReceiveQueue{
		IdentifyMsg: HandleMadesSendEventMessage,
		Queue:       NewQueue(b.messageTracker, deliveryTracker, receivedTracker),
		receivers: []*amqp.Receiver{
			b.outboxConn.receivers[1],
		},
	}

	return queue
}

func (b *Bencher) CreateQueueEndpointSendEventTracing(trackID int) *ReceiveQueue {
	receivedTracker := NewTrackable()
	receivedTracker.Name = "Received event"
	receivedTracker.EstimatedEventSize = 100 // Roughly
	receivedTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID] = receivedTracker

	queue := &ReceiveQueue{
		IdentifyMsg: HandleMadesSendEventMessage,
		Queue:       NewQueue(b.messageTracker, receivedTracker),
		receivers: []*amqp.Receiver{
			b.outboxConn.receivers[1],
		},
	}

	return queue
}

func (b *Bencher) CreateQueueEndpointInbox(trackID int) *ReceiveQueue {
	inboxTracker := NewTrackable()
	inboxTracker.Name = "Received in inbox"
	inboxTracker.EstimatedEventSize = b.payloadSize
	inboxTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID] = inboxTracker

	queue := &ReceiveQueue{
		IdentifyMsg: HandleMadesInboxMessage,
		Queue:       NewQueue(b.messageTracker, inboxTracker),
		receivers: []*amqp.Receiver{
			b.inboxConn.receivers[0],
		},
	}

	return queue
}

func (b *Bencher) CreateQueueToolboxOutboxReply(trackID int) *ReceiveQueue {
	replyTracker := NewTrackable()
	replyTracker.Name = "Received reply"
	replyTracker.EstimatedEventSize = 100 // Roughly
	replyTracker.Limit = b.payloadCount
	b.tracker.Trackables[trackID] = replyTracker

	queue := &ReceiveQueue{
		IdentifyMsg: HandleToolboxReplyMessage,
		Queue:       NewQueue(b.messageTracker, replyTracker),
		receivers: []*amqp.Receiver{
			b.outboxConn.receivers[0],
		},
	}

	return queue
}

func (b *Bencher) CleanQueues() {
	receivers := []*amqp.Receiver{}
	if b.outboxConn != nil && b.outboxConn.receivers != nil {
		receivers = append(receivers, b.outboxConn.receivers...)
	}
	if b.inboxConn != nil && b.inboxConn.receivers != nil {
		receivers = append(receivers, b.inboxConn.receivers...)
	}
	cleanReceivers(receivers...)
}

func (b *Bencher) ConfigureForEndpoint() {
	b.outboxConnOpts = &AMQPConnOpts{
		socketAddr: b.outboxSocketAddr,
		sendQueues: []string{b.outbox},
		recvQueues: []string{b.outboxReply, b.sendEvent, b.outbox},
	}
	b.inboxConnOpts = &AMQPConnOpts{
		socketAddr: b.inboxSocketAddr,
		recvQueues: []string{b.inbox},
	}

	b.ConnectQueues()
	b.CleanQueues()
	// Close outbox receiver
	_ = b.outboxConn.receivers[2].Close(context.Background())

	b.ConfigureTracker(5, b.payloadSize)
	b.messageTracker = NewMessageTracker()
	ch := make(MessageEventListener, 10000)

	outboxQueue := b.CreateQueueEndpointOutbox(0, false, 0b1111)
	outboxQueue.sendChan = ch

	outboxReplyQueue := b.CreateQueueEndpointOutboxReply(1)
	inboxQueue := b.CreateQueueEndpointInbox(2)

	sendEventQueue := b.CreateQueueEndpointSendEvent(3)
	sendEventQueue.listenerType = 1 // receive event
	sendEventQueue.listener = ch

	b.queues = &Queues{
		SenderQueue: outboxQueue,
		ReceiveQueues: []*ReceiveQueue{
			outboxReplyQueue,
			inboxQueue,
			sendEventQueue,
		},
	}
}

func (b *Bencher) ConfigureForEndpointTracing() {
	b.outboxConnOpts = &AMQPConnOpts{
		socketAddr: b.outboxSocketAddr,
		sendQueues: []string{b.outbox},
		recvQueues: []string{b.outboxReply, b.sendEvent, b.outbox},
	}

	b.ConnectQueues()
	b.CleanQueues()
	// Close outbox receiver
	_ = b.outboxConn.receivers[2].Close(context.Background())

	b.ConfigureTracker(3, 1)
	b.messageTracker = NewMessageTracker()
	ch := make(MessageEventListener, 10000)

	outboxQueue := b.CreateQueueEndpointOutbox(0, true, 0b111)
	outboxQueue.sendChan = ch

	outboxReplyQueue := b.CreateQueueEndpointOutboxReply(1)

	sendEventQueue := b.CreateQueueEndpointSendEventTracing(2)
	sendEventQueue.listener = ch

	b.queues = &Queues{
		SenderQueue: outboxQueue,
		ReceiveQueues: []*ReceiveQueue{
			outboxReplyQueue,
			sendEventQueue,
		},
	}
}

func (b *Bencher) ConfigureForAMQP() {
	b.outboxConnOpts = &AMQPConnOpts{
		socketAddr: b.inboxSocketAddr,
		sendQueues: []string{b.inbox},
	}
	b.inboxConnOpts = &AMQPConnOpts{
		socketAddr: b.inboxSocketAddr,
		recvQueues: []string{b.inbox},
	}

	b.ConnectQueues()
	b.CleanQueues()

	b.ConfigureTracker(2, b.payloadSize)
	b.messageTracker = NewMessageTracker()
	ch := make(MessageEventListener, 10000)

	outboxQueue := b.CreateQueueEndpointOutbox(0, false, 0b10)
	outboxQueue.sendChan = ch

	inboxReceivers := make([]*ReceiveQueue, b.goroutines)
	for i := 0; i < int(b.goroutines); i++ {
		inboxQueue := b.CreateQueueEndpointInbox(i + 1)
		inboxQueue.listener = ch
		inboxReceivers[i] = inboxQueue
	}

	b.queues = &Queues{
		SenderQueue:   outboxQueue,
		ReceiveQueues: inboxReceivers,
	}

}
func (b *Bencher) ConfigureForToolbox() {
	b.outboxConnOpts = &AMQPConnOpts{
		socketAddr: b.outboxSocketAddr,
		sendQueues: []string{b.outbox},
		recvQueues: []string{b.outboxReply, b.outbox},
	}
	b.inboxConnOpts = &AMQPConnOpts{
		socketAddr: b.inboxSocketAddr,
		recvQueues: []string{b.inbox},
	}

	b.ConnectQueues()
	b.CleanQueues()
	// Close outbox receiver
	_ = b.outboxConn.receivers[1].Close(context.Background())

	b.ConfigureTracker(3, b.payloadSize)
	b.messageTracker = NewMessageTracker()
	ch := make(MessageEventListener, 10000)

	outboxQueue := b.CreateQueueEndpointOutbox(0, false, 0b11)
	outboxQueue.sendChan = ch

	inboxQueue := b.CreateQueueEndpointInbox(1)

	outboxReplyQueue := b.CreateQueueToolboxOutboxReply(2)
	outboxReplyQueue.listener = ch

	b.queues = &Queues{
		SenderQueue: outboxQueue,
		ReceiveQueues: []*ReceiveQueue{
			outboxReplyQueue,
			inboxQueue,
		},
	}
}

func (bencher *Bencher) startBenching() {
	go func() {
		for i := 1; bencher.tracker.End.IsZero(); i++ {
			log.Printf("Intermediate Report #%d\n", i)
			bencher.tracker.BenchResults(os.Stdout)
			time.Sleep(time.Second * 2)
		}
	}()

	bencher.tracker.SetStart(time.Now())
	bencher.queues.Start()
	bencher.tracker.AwaitEnd()
}

/*func (bencher *Bencher) printMissing() {
	missingEvents := make([]string, 0, 5)
	for id, msg := range bencher.messageStatsByMsgID {
		if msg.timeCreated.IsZero() {
			missingEvents = append(missingEvents, "CREATED")
		}
		if msg.timeAccepted.IsZero() {
			missingEvents = append(missingEvents, "ACCEPTED")
		}
		if msg.timeReceived.IsZero() {
			missingEvents = append(missingEvents, "RECEIVED")
		}
		if msg.timeDeliveredEvent.IsZero() {
			missingEvents = append(missingEvents, "ACK_DELIVERED")
		}
		if msg.timeReceivedEvent.IsZero() {
			missingEvents = append(missingEvents, "ACK_RECEIVED")
		}
		if len(missingEvents) > 0 {
			fmt.Printf("MessageID: %s missing events: [%s]\n", id, strings.Join(missingEvents, ", "))
			// Reset array
			missingEvents = missingEvents[:0]
		}
	}

	for id, msg := range bencher.messageStatsByBaMsgID {
		if msg.timeCreated.IsZero() {
			missingEvents = append(missingEvents, "CREATED")
		}
		if msg.timeAccepted.IsZero() {
			missingEvents = append(missingEvents, "ACCEPTED")
		}
		if msg.timeReceived.IsZero() {
			missingEvents = append(missingEvents, "RECEIVED")
		}
		if msg.timeDeliveredEvent.IsZero() {
			missingEvents = append(missingEvents, "ACK_DELIVERED")
		}
		if msg.timeReceivedEvent.IsZero() {
			missingEvents = append(missingEvents, "ACK_RECEIVED")
		}
		if len(missingEvents) > 0 {
			fmt.Printf("BAMessageID: %s missing events: [%s]\n", id, strings.Join(missingEvents, ", "))
			// Reset array
			missingEvents = missingEvents[:0]
		}
	}
}*/

func (bencher *Bencher) close() {
	if bencher.outboxConn != nil {
		err := bencher.outboxConn.Close()
		if err != nil {
			log.Printf("unable to close outbox receiver amqp connection. err: %s\n", err.Error())
		}
	}

	if bencher.inboxConn != nil {
		err := bencher.inboxConn.Close()
		if err != nil {
			log.Printf("unable to close inbox sender amqp connection. err: %s\n", err.Error())
		}
	}
}
