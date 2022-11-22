package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

type Bencher struct {
	m sync.Mutex

	verbose      bool
	payloadSize  uint64
	payloadCount uint64
	receiverCode string
	messageType  string
	goroutines   uint64
	maxInTransit uint64

	outboxSocketAddr    string
	outboxAmqpUser      string
	outboxAmqpPass      string
	outboxReply         string
	outbox              string
	sendEvent           string
	outboxClient        *amqp.Client
	outboxSender        *amqp.Sender
	outboxReplyReceiver *amqp.Receiver
	sendEventReceiver   *amqp.Receiver

	inbox           string
	inboxSocketAddr string
	inboxAmqpUser   string
	inboxAmqpPass   string
	inboxClient     *amqp.Client
	inboxReceiver   *amqp.Receiver

	senderWg        sync.WaitGroup
	receiverWg      sync.WaitGroup
	deliveryEventWg sync.WaitGroup
	receiverEventWg sync.WaitGroup

	messagesToSend chan uint64

	timeStart            time.Time
	timeEndOutbox        time.Time
	timeEndSent          time.Time
	timeEndReceive       time.Time
	timeEndDeliveryEvent time.Time
	timeEndReceiveEvent  time.Time

	outboxCounter        uint64
	sentCounter          uint64
	receivedCounter      uint64
	deliveryEventCounter uint64
	receivedEventCounter uint64

	messageStatsByBaMsgID map[string]*MessageStats
	messageStatsByMsgID   map[string]*MessageStats

	lastReceivedTimestamp time.Time

	statistics FlightTimeStatistics
}

func NewBencher() *Bencher {
	return &Bencher{
		statistics: FlightTimeStatistics{
			Outbox: FlightTimeStats{
				buckets: NewSortedMap[int64, int64](),
			},
			Sent: FlightTimeStats{
				buckets: NewSortedMap[int64, int64](),
			},
			Received: FlightTimeStats{
				buckets: NewSortedMap[int64, int64](),
			},
			DeliveryEvent: FlightTimeStats{
				buckets: NewSortedMap[int64, int64](),
			},
			ReceivedEvent: FlightTimeStats{
				buckets: NewSortedMap[int64, int64](),
			},
		},
	}
}

type MessageStats struct {
	ID                 int32
	timeCreated        time.Time
	timeAccepted       time.Time
	timeReceived       time.Time
	timeDeliveredEvent time.Time
	timeReceivedEvent  time.Time
}

func (bencher *Bencher) prepareOutbox() {
	// Create outboxClient
	outboxClientOpts := []amqp.ConnOption{
		amqp.ConnSASLPlain(bencher.outboxAmqpUser, bencher.outboxAmqpPass),
	}
	if strings.HasPrefix(bencher.outboxSocketAddr, "amqps://") {
		// #nosec G402 - Ignore InsecureSkipVerify: true. This is a benchmark tool, no need to enforce strict security checking
		outboxClientOpts = append(outboxClientOpts, amqp.ConnTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}

	outboxClient, err := amqp.Dial(bencher.outboxSocketAddr, outboxClientOpts...)
	if err != nil {
		log.Fatal("Dialing AMQP outbox server:", err)
	}

	sendSession, err := outboxClient.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP outbox sender session:", err)
	}

	recvSession, err := outboxClient.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP outbox receive session:", err)
	}

	// Create outbox sender
	bencher.outboxSender, err = sendSession.NewSender(
		amqp.LinkTargetAddress(bencher.outbox),
	)
	if err != nil {
		log.Fatal("Creating outbox sender link:", err)
	}

	// Create outbox reply receiver
	bencher.outboxReplyReceiver, err = recvSession.NewReceiver(
		amqp.LinkSourceAddress(bencher.outboxReply),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating outbox receiver link:", err)
	}

	// Create send event receiver
	bencher.sendEventReceiver, err = recvSession.NewReceiver(
		amqp.LinkSourceAddress(bencher.sendEvent),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating send event receiver link:", err)
	}
}

func (bencher *Bencher) prepareInbox() {
	// Create inboxClient
	inboxClientOpts := []amqp.ConnOption{
		amqp.ConnSASLPlain(bencher.inboxAmqpUser, bencher.inboxAmqpPass),
	}
	if strings.HasPrefix(bencher.inboxSocketAddr, "amqps://") {
		// #nosec G402 - Ignore InsecureSkipVerify: true. This is a benchmark tool, no need to enforce strict security checking
		inboxClientOpts = append(inboxClientOpts, amqp.ConnTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}

	inboxClient, err := amqp.Dial(bencher.inboxSocketAddr, inboxClientOpts...)
	if err != nil {
		log.Fatal("Dialing AMQP inbox server:", err)
	}

	recvSession, err := inboxClient.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP inbox receive session:", err)
	}

	// Create a receiver
	bencher.inboxReceiver, err = recvSession.NewReceiver(
		amqp.LinkSourceAddress(bencher.inbox),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating inbox receiver link:", err)
	}
}

func (bencher *Bencher) cleanQueues() {
	wg := sync.WaitGroup{}
	wg.Add(3)
	go bencher.cleanQueue(&wg, bencher.inboxReceiver)
	go bencher.cleanQueue(&wg, bencher.outboxReplyReceiver)
	go bencher.cleanQueue(&wg, bencher.sendEventReceiver)
	wg.Wait()
}

func (bencher *Bencher) cleanQueue(wg *sync.WaitGroup, receiver *amqp.Receiver) {
	// Drain  outbox reply before testing
	log.Printf("Cleaning %s before commencing test...\n", receiver.Address())
	var i uint64
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		msg, err := receiver.Receive(ctx)
		cancel()
		if err != nil {
			break
		}
		go receiver.AcceptMessage(context.Background(), msg)
		i++
		if i%100 == 0 {
			log.Printf("Cleaned %d messages from queue %s so far\n", i, receiver.Address())
		}
	}
	log.Printf("Cleaned %d messages from queue %s\n", i, receiver.Address())
	wg.Done()
}

func (bencher *Bencher) startBenching() {
	bencher.timeStart = time.Now()
	bencher.receiverWg.Add(int(bencher.payloadCount))
	bencher.deliveryEventWg.Add(int(bencher.payloadCount))
	bencher.receiverEventWg.Add(int(bencher.payloadCount))
	bencher.messageStatsByBaMsgID = map[string]*MessageStats{}
	bencher.messageStatsByMsgID = map[string]*MessageStats{}

	log.Printf("Starting %s\n", bencher.timeStart.String())

	bencher.sendMessages()
	go bencher.receiveMessages(bencher.outboxReplyReceiver)
	go bencher.receiveMessages(bencher.inboxReceiver)
	go bencher.receiveMessages(bencher.sendEventReceiver)

	go func() {
		for i := 1; bencher.timeEndReceiveEvent.IsZero(); i++ {
			bencher.m.Lock()
			log.Printf("Intermediate Report #%d\n", i)
			bencher.printBenchResults(false)
			bencher.m.Unlock()
			time.Sleep(time.Second * 2)
		}
	}()

	go func() {
		bencher.senderWg.Wait()
		bencher.timeEndOutbox = time.Now()
	}()

	bencher.senderWg.Wait()
	bencher.receiverEventWg.Wait()
	bencher.timeEndReceiveEvent = time.Now()

	for {
		log.Println("waiting a second for any out of order messages...")
		time.Sleep(time.Second)

		if time.Since(bencher.lastReceivedTimestamp) > time.Second {
			break
		}
	}
}

func (bencher *Bencher) sendMessages() {
	initialMessagesToSend := bencher.payloadCount
	if bencher.maxInTransit > 0 {
		initialMessagesToSend = bencher.maxInTransit
	}
	bencher.messagesToSend = make(chan uint64, initialMessagesToSend)
	for i := uint64(1); i <= initialMessagesToSend; i++ {
		bencher.messagesToSend <- i
	}
	var m sync.Mutex
	for k := uint64(0); k < bencher.goroutines; k++ {
		workerID := k
		bencher.senderWg.Add(1)
		go func() {
			log.Printf("starting outbox worker #%d\n", workerID)
			defer bencher.senderWg.Done()

			for {
				// Await for message to send
				m.Lock()
				if bencher.outboxCounter >= bencher.payloadCount {
					m.Unlock()
					break
				}
				n := <-bencher.messagesToSend
				_ = atomic.AddUint64(&bencher.outboxCounter, 1)
				m.Unlock()

				payload := make([]byte, bencher.payloadSize)
				fillPayloadWithData(payload)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
				creationTime := time.Now()
				baMessageID := fmt.Sprintf("%d-%s", n, uuid.New().String())
				msg := &amqp.Message{
					Data: [][]byte{payload},
					ApplicationProperties: map[string]interface{}{
						"receiverCode":      bencher.receiverCode,
						"messageType":       bencher.messageType,
						"baMessageID":       baMessageID,
						"senderApplication": "Go-MADES-Bench",
					},
					Properties: &amqp.MessageProperties{
						CreationTime:  &creationTime,
						CorrelationID: baMessageID,
					},
				}
				bencher.m.Lock()
				bencher.messageStatsByBaMsgID[baMessageID] = &MessageStats{
					ID:          int32(n),
					timeCreated: creationTime,
				}
				bencher.m.Unlock()
				if bencher.verbose {
					log.Printf("#     created      : %d", n)
				}

				// Send message to outbox
				err := bencher.outboxSender.Send(ctx, msg)
				if err != nil {
					log.Fatal("Sending message:", err)
				}
				cancel()
			}
		}()
	}
}

func (bencher *Bencher) receiveMessages(receiver *amqp.Receiver) {
	ctx := context.Background()
	for {
		// Receive next message
		msg, err := receiver.Receive(ctx)
		if err != nil {
			log.Fatal("Reading message from AMQP:", err)
		}

		bencher.handleMessage(msg, receiver.Address())

		// Accept message
		err = receiver.AcceptMessage(ctx, msg)
		if err != nil {
			log.Fatal("Accepting message from AMQP:", err)
		}
	}
}

func (bencher *Bencher) identifyMsgStats(msg *amqp.Message) (*MessageStats, string) {
	var stats *MessageStats
	var correlationID, messageID string
	var ok bool

	bencher.m.Lock()
	// Correlation id is used for outbox.reply events
	if msg.Properties != nil && msg.Properties.CorrelationID != nil {
		correlationID = msg.Properties.CorrelationID.(string)
		stats = bencher.messageStatsByBaMsgID[correlationID]
		delete(bencher.messageStatsByBaMsgID, correlationID)
	}
	if msg.ApplicationProperties != nil {
		if stats == nil {
			if messageID, ok = msg.ApplicationProperties["baMessageID"].(string); ok {
				stats = bencher.messageStatsByBaMsgID[messageID]
				delete(bencher.messageStatsByBaMsgID, messageID)
			}
		}

		if messageID, ok = msg.ApplicationProperties["messageID"].(string); ok {
			statsByID, ok := bencher.messageStatsByMsgID[messageID]
			if !ok {
				bencher.messageStatsByMsgID[messageID] = stats
			} else {
				stats = statsByID
			}
		}
	}
	bencher.m.Unlock()

	return stats, messageID
}

func (bencher *Bencher) handleMessage(msg *amqp.Message, queue string) {
	bencher.lastReceivedTimestamp = time.Now()
	stats, messageID := bencher.identifyMsgStats(msg)

	if stats == nil {
		log.Printf("unknown amqp message %#v", msg.ApplicationProperties)
		return
	}

	now := time.Now()
	dur := now.Sub(stats.timeCreated)

	switch queue {
	case bencher.outboxReply:
		if !stats.timeAccepted.IsZero() {
			log.Printf("message %s is already sent\n", messageID)
			break
		}
		stats.timeAccepted = now
		bencher.statistics.Sent.Add(dur)
		bencher.sentCounter++
		if msg.ApplicationProperties["errorCode"] != nil {
			log.Printf("##    ERROR        : %d,\t          \t msgID: %s\n, error: %v", stats.ID, messageID, msg.ApplicationProperties)
		} else if bencher.verbose {
			log.Printf("##    sent         : %d,\t          \t msgID: %s\n", stats.ID, messageID)
		}
		if bencher.sentCounter == bencher.payloadCount {
			bencher.timeEndSent = now
		}

	case bencher.inbox:
		if !stats.timeReceived.IsZero() {
			log.Printf("message %s is already received\n", messageID)
			break
		}
		stats.timeReceived = now
		bencher.statistics.Received.Add(dur)
		bencher.receivedCounter++
		if bencher.verbose {
			log.Printf("###   received     : %d,\t total: %d,\t msgID: %s\n", stats.ID, bencher.receivedCounter, messageID)
		}
		bencher.receiverWg.Done()
		if bencher.receivedCounter == bencher.payloadCount {
			bencher.timeEndReceive = now
		}

	case bencher.sendEvent:
		switch msg.Value.(string) {
		case "DELIVERED":
			if !stats.timeDeliveredEvent.IsZero() {
				log.Printf("message %s event is already delivered\n", messageID)
				break
			}
			stats.timeDeliveredEvent = now
			bencher.statistics.DeliveryEvent.Add(dur)
			bencher.deliveryEventCounter++
			if bencher.verbose {
				log.Printf("####  delivered ack: %d,\t total: %d,\t msgID: %s\n", stats.ID, bencher.deliveryEventCounter, messageID)
			}
			bencher.deliveryEventWg.Done()
			if bencher.deliveryEventCounter == bencher.payloadCount {
				bencher.timeEndDeliveryEvent = now
			}

		case "RECEIVED":
			if !stats.timeReceivedEvent.IsZero() {
				log.Printf("message %s event is already received\n", messageID)
				break
			}
			stats.timeReceivedEvent = now
			bencher.statistics.ReceivedEvent.Add(dur)
			bencher.receivedEventCounter++
			bencher.messagesToSend <- bencher.payloadCount - bencher.outboxCounter
			if bencher.verbose {
				log.Printf("##### received  ack: %d,\t total: %d,\t msgID: %s\n", stats.ID, bencher.receivedEventCounter, messageID)
			}
			bencher.receiverEventWg.Done()
			if bencher.receivedEventCounter == bencher.payloadCount {
				bencher.timeEndReceiveEvent = now
			}

		default:
			log.Printf("unknown send event value: %s\n", msg.Value)
		}
	}

	// Cleanup if no linger needed
	if !stats.timeAccepted.IsZero() &&
		!stats.timeCreated.IsZero() &&
		!stats.timeDeliveredEvent.IsZero() &&
		!stats.timeReceived.IsZero() &&
		!stats.timeReceivedEvent.IsZero() {
		bencher.m.Lock()
		delete(bencher.messageStatsByMsgID, messageID)
		bencher.m.Unlock()
	}
}

func (bencher *Bencher) duration(start time.Time, end time.Time) time.Duration {
	if end.IsZero() {
		if !bencher.timeEndReceiveEvent.IsZero() {
			return bencher.timeEndReceiveEvent.Sub(start)
		}
		return time.Since(start)
	}
	return end.Sub(start)
}

func (bencher *Bencher) printBenchResults(printMissing bool) {
	outboxDuration := bencher.duration(bencher.timeStart, bencher.timeEndOutbox)
	sentDuration := bencher.duration(bencher.timeStart, bencher.timeEndSent)
	receivedDuration := bencher.duration(bencher.timeStart, bencher.timeEndReceive)
	deliveryEventDuration := bencher.duration(bencher.timeStart, bencher.timeEndDeliveryEvent)
	receivedEventDuration := bencher.duration(bencher.timeStart, bencher.timeEndReceiveEvent)
	bencher.statistics.calcMedians()

	estimatedAckSize := 100

	log.Printf("==============================================================================")
	log.Printf("Started at       : %s\n", bencher.timeStart.Format(time.RFC3339))
	if !bencher.timeEndReceiveEvent.IsZero() {
		log.Printf("Ended at         : %s\n", bencher.timeEndReceiveEvent.Format(time.RFC3339))
	}
	log.Printf("Duration         : %s\n", receivedEventDuration.String())
	log.Printf("Msg size         : %d bytes\n", bencher.payloadSize)
	log.Printf("\n")
	log.Printf("=== Statistics\n")
	log.Printf("Sent to outbox   : %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t -- not applicable --\n",
		float64(bencher.outboxCounter)/outboxDuration.Seconds(),
		float64(bencher.outboxCounter)/outboxDuration.Seconds()*float64(bencher.payloadSize)/1000000,
		bencher.outboxCounter,
		bencher.payloadCount,
		outboxDuration.Round(time.Millisecond),
	)
	log.Printf("Sent to broker   : %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3f (avg) %.3f (median)\n",
		float64(bencher.sentCounter)/sentDuration.Seconds(),
		float64(bencher.sentCounter)/sentDuration.Seconds()*float64(bencher.payloadSize)/1000000,
		bencher.sentCounter,
		bencher.payloadCount,
		sentDuration.Round(time.Millisecond),
		bencher.statistics.Sent.Average.Seconds(),
		bencher.statistics.Sent.Median.Seconds(),
	)
	log.Printf("Received in inbox: %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3f (avg) %.3f (median)\n",
		float64(bencher.receivedCounter)/receivedDuration.Seconds(),
		float64(bencher.receivedCounter)/receivedDuration.Seconds()*float64(bencher.payloadSize)/1000000,
		bencher.receivedCounter,
		bencher.payloadCount,
		receivedDuration.Round(time.Millisecond),
		bencher.statistics.Received.Average.Seconds(),
		bencher.statistics.Received.Median.Seconds(),
	)
	log.Printf("Delivery Event   : %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3fs (avg) %.3fs (median)\n",
		float64(bencher.deliveryEventCounter)/deliveryEventDuration.Seconds(),
		float64(bencher.deliveryEventCounter)/deliveryEventDuration.Seconds()*float64(estimatedAckSize)/1000000,
		bencher.deliveryEventCounter,
		bencher.payloadCount,
		deliveryEventDuration.Round(time.Millisecond),
		bencher.statistics.DeliveryEvent.Average.Seconds(),
		bencher.statistics.DeliveryEvent.Median.Seconds(),
	)
	log.Printf("Received Event   : %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3fs (avg) %.3fs (median)\n",
		float64(bencher.receivedEventCounter)/receivedEventDuration.Seconds(),
		float64(bencher.receivedEventCounter)/receivedEventDuration.Seconds()*float64(estimatedAckSize)/1000000,
		bencher.receivedEventCounter,
		bencher.payloadCount,
		receivedEventDuration.Round(time.Millisecond),
		bencher.statistics.ReceivedEvent.Average.Seconds(),
		bencher.statistics.ReceivedEvent.Median.Seconds(),
	)

	log.Printf("==============================================================================")
}

func (bencher *Bencher) close() {
	if bencher.outboxClient != nil {
		err := bencher.outboxClient.Close()
		if err != nil {
			log.Printf("unable to close outbox receiver amqp connection. err: %s\n", err.Error())
		}
	}
	if bencher.outboxSender != nil {
		err := bencher.outboxSender.Close(context.Background())
		if err != nil {
			log.Printf("unable to close outbox sender amqp connection. err: %s\n", err.Error())
		}
	}

	if bencher.inboxClient != nil {
		err := bencher.inboxClient.Close()
		if err != nil {
			log.Printf("unable to close inbox sender amqp connection. err: %s\n", err.Error())
		}
	}
	if bencher.inboxReceiver != nil {
		err := bencher.inboxReceiver.Close(context.Background())
		if err != nil {
			log.Printf("unable to close inbox receiver amqp connection. err: %s\n", err.Error())
		}
	}
}

func fillPayloadWithData(data []byte) {
	// #nosec G404 - Intentionally using weak random number generator math/rand, as this is only for random payload data
	// No need to waste cpu time with crypto/rand
	_, err := rand.Read(data)
	if err != nil {
		log.Fatal("Creating pseudo random payload:", err)
	}
}
