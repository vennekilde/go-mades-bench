package main

import (
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

	// Outbox AMQP senders/receivers
	outboxConn          *AMQPConn
	outboxSender        *amqp.Sender
	outboxReplyReceiver *amqp.Receiver
	sendEventReceiver   *amqp.Receiver

	// Inbox AMQP receiver
	inboxConn     *AMQPConn
	inboxReceiver *amqp.Receiver

	tracker *Tracker
	queues  *Queues
}

func NewBencher() *Bencher {
	return &Bencher{}
}

// prepareOutbox connects the the sender endpoint broker and sets up senders for the outbox and
// receivers for the send event and outbox reply queues
func (bencher *Bencher) prepareOutbox() {
	var err error
	bencher.outboxConn, err = newAMQPConn(&AMQPConnOpts{
		socketAddr: bencher.outboxSocketAddr,
		sendQueues: []string{bencher.outbox},
		recvQueues: []string{bencher.outboxReply, bencher.sendEvent, bencher.outbox},
		username:   bencher.outboxAmqpUser,
		password:   bencher.outboxAmqpPass,
	})

	if err != nil {
		log.Fatal("Unable to create outbox AMQP client:", err)
	}

	bencher.outboxSender = bencher.outboxConn.senders[0]
	bencher.outboxReplyReceiver = bencher.outboxConn.receivers[0]
	bencher.sendEventReceiver = bencher.outboxConn.receivers[1]
}

// prepareOutbox connects the the receiver endpoint broker and sets up receiver for the inbox queue
func (bencher *Bencher) prepareInbox() {
	var err error
	bencher.inboxConn, err = newAMQPConn(&AMQPConnOpts{
		socketAddr: bencher.inboxSocketAddr,
		recvQueues: []string{bencher.inbox},
		username:   bencher.inboxAmqpUser,
		password:   bencher.inboxAmqpPass,
	})

	if err != nil {
		log.Fatal("Unable to create inbox AMQP client:", err)
	}

	bencher.inboxReceiver = bencher.inboxConn.receivers[0]
}

func (b *Bencher) ConfigureForEndpoint() {
	b.tracker = &Tracker{
		PayloadSize: int(b.payloadSize),
		Trackables:  make([]*Trackable, 5),
	}

	sendTracker := NewTrackable()
	sendTracker.Name = "Sent to outbox"
	sendTracker.EstimatedEventSize = b.payloadSize
	sendTracker.Limit = b.payloadCount
	b.tracker.Trackables[0] = sendTracker

	replyTracker := NewTrackable()
	replyTracker.Name = "Sent to broker"
	replyTracker.EstimatedEventSize = b.payloadSize
	replyTracker.Limit = b.payloadCount
	b.tracker.Trackables[1] = replyTracker

	inboxTracker := NewTrackable()
	inboxTracker.Name = "Received in inbox"
	inboxTracker.EstimatedEventSize = b.payloadSize
	inboxTracker.Limit = b.payloadCount
	b.tracker.Trackables[2] = inboxTracker

	deliveryTracker := NewTrackable()
	deliveryTracker.Name = "Delivery event"
	deliveryTracker.EstimatedEventSize = 100 // Roughly
	deliveryTracker.Limit = b.payloadCount
	b.tracker.Trackables[3] = deliveryTracker

	receivedTracker := NewTrackable()
	receivedTracker.Name = "Received event"
	receivedTracker.EstimatedEventSize = 100 // Roughly
	receivedTracker.Limit = b.payloadCount
	b.tracker.Trackables[4] = receivedTracker

	msgTracker := NewMessageTracker()

	ch := make(MessageEventListener, 10000)

	b.queues = &Queues{
		SenderQueue: &SenderQueue{
			CreateMsg:         CreateMadesMsgCreator(b.receiverCode, b.messageType),
			PayloadSize:       b.payloadSize,
			Queue:             NewQueue(msgTracker, sendTracker),
			maxUnacknowledged: b.maxInTransit,
			sender:            b.outboxConn.senders[0],
			sendChan:          ch,
			senderCount:       int(b.goroutines),
		},
		ReceiveQueues: []*ReceiveQueue{
			&ReceiveQueue{
				IdentifyMsg: HandleMadesReplyMessage,
				Queue:       NewQueue(msgTracker, replyTracker),
				receiver:    b.outboxConn.receivers[0],
			},
			&ReceiveQueue{
				IdentifyMsg: HandleMadesInboxMessage,
				Queue:       NewQueue(msgTracker, inboxTracker),
				receiver:    b.inboxConn.receivers[0],
			},
			&ReceiveQueue{
				IdentifyMsg: HandleMadesSendEventMessage,
				Queue:       NewQueue(msgTracker, deliveryTracker, receivedTracker),
				receiver:    b.outboxConn.receivers[1],
			},
		},
	}
	b.queues.ReceiveQueues[2].listenerType = 1 // receive event
	b.queues.ReceiveQueues[2].listener = ch
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
