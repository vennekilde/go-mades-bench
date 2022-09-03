package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
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
}

type MessageStats struct {
	ID                 int32
	timeCreated        time.Time
	timeAccepted       time.Time
	timeReceived       time.Time
	timeDeliveredEvent time.Time
	timeReceivedEvent  time.Time
}

type FlightTimeStatistics struct {
	Outbox        FlightTimeStats
	Sent          FlightTimeStats
	DeliveryEvent FlightTimeStats
	ReceivedEvent FlightTimeStats
	Received      FlightTimeStats
}

type FlightTimeStats struct {
	Average time.Duration
	Median  time.Duration
}

func main() {
	bencher := Bencher{}
	flag.BoolVar(&bencher.verbose, "v", false, "Print all message events")

	flag.Uint64Var(&bencher.payloadSize, "size", 1000000, "Incompressible payload size to generate") //1000000 = 1mb,
	flag.Uint64Var(&bencher.payloadCount, "n", 10000, "Number of messages to send")
	flag.Uint64Var(&bencher.maxInTransit, "max-in-transit", 0, "Max messages allowed in transit. max-in-transit <= 0 means unlimited")
	flag.Uint64Var(&bencher.goroutines, "goroutines", uint64(runtime.NumCPU()), "Number of go routines to use when sending")
	flag.StringVar(&bencher.receiverCode, "receiver", "ecp-endpoint", "Receiver Component Code")
	flag.StringVar(&bencher.messageType, "message-type", "TEST-MESSAGE", "Message type to send messages with")

	flag.StringVar(&bencher.sendEvent, "outbox-send-event", "ecp.endpoint.send.event", "send event queue")
	flag.StringVar(&bencher.outboxReply, "outbox-reply", "ecp.endpoint.outbox.reply", "outbox reply queue")
	flag.StringVar(&bencher.outbox, "outbox", "ecp.endpoint.outbox", "outbox queue")
	flag.StringVar(&bencher.outboxSocketAddr, "outbox-addr", "amqp://localhost:5672", "Socket address to reach the internal broker")
	flag.StringVar(&bencher.outboxAmqpUser, "outbox-user", "endpoint", "Outbox broker username")
	flag.StringVar(&bencher.outboxAmqpPass, "outbox-pass", "password", "Outbox broker password")

	flag.StringVar(&bencher.inbox, "inbox", "ecp.endpoint.inbox", "inbox queue")
	flag.StringVar(&bencher.inboxSocketAddr, "inbox-addr", "amqp://localhost:5672", "Socket address to reach the internal broker")
	flag.StringVar(&bencher.inboxAmqpUser, "inbox-user", "endpoint", "Inbox broker password")
	flag.StringVar(&bencher.inboxAmqpPass, "inbox-pass", "password", "Inbox broker username")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	defer bencher.close()
	bencher.prepareOutbox()
	bencher.prepareInbox()
	bencher.cleanQueues()
	bencher.startBenching()

	log.Printf("\n")
	log.Printf("Final Report\n")
	bencher.printBenchResults(true)
}

func (bencher *Bencher) prepareOutbox() {
	// Create outboxClient
	outboxClientOpts := []amqp.ConnOption{
		amqp.ConnSASLPlain(bencher.outboxAmqpUser, bencher.outboxAmqpPass),
	}
	if strings.HasPrefix(bencher.outboxSocketAddr, "amqps://") {
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
		receiver.AcceptMessage(context.Background(), msg)
		i++
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
		time.Sleep(time.Second * 2)
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
				m.Unlock()
				n := <-bencher.messagesToSend
				_ = atomic.AddUint64(&bencher.outboxCounter, 1)

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
					timeCreated: time.Now(),
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
	}
	if msg.ApplicationProperties != nil {
		if stats == nil {
			if messageID, ok = msg.ApplicationProperties["baMessageID"].(string); ok {
				stats = bencher.messageStatsByBaMsgID[messageID]
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

	switch queue {
	case bencher.outboxReply:
		if !stats.timeAccepted.IsZero() {
			log.Printf("message %s is already sent\n", messageID)
			break
		}
		stats.timeAccepted = time.Now()
		bencher.sentCounter++
		if msg.ApplicationProperties["errorCode"] != nil {
			log.Printf("##    ERROR        : %d,\t          \t msgID: %s\n, error: %v", stats.ID, messageID, msg.ApplicationProperties)
		} else if bencher.verbose {
			log.Printf("##    sent         : %d,\t          \t msgID: %s\n", stats.ID, messageID)
		}
		if bencher.sentCounter == bencher.payloadCount {
			bencher.timeEndSent = time.Now()
		}

	case bencher.inbox:
		if !stats.timeReceived.IsZero() {
			log.Printf("message %s is already received\n", messageID)
			break
		}
		stats.timeReceived = time.Now()
		bencher.receivedCounter++
		if bencher.verbose {
			log.Printf("###   received     : %d,\t total: %d,\t msgID: %s\n", stats.ID, bencher.receivedCounter, messageID)
		}
		bencher.receiverWg.Done()
		if bencher.receivedCounter == bencher.payloadCount {
			bencher.timeEndReceive = time.Now()
		}

	case bencher.sendEvent:
		switch msg.Value.(string) {
		case "DELIVERED":
			if !stats.timeDeliveredEvent.IsZero() {
				log.Printf("message %s event is already delivered\n", messageID)
				break
			}
			stats.timeDeliveredEvent = time.Now()
			bencher.deliveryEventCounter++
			if bencher.verbose {
				log.Printf("####  delivered ack: %d,\t total: %d,\t msgID: %s\n", stats.ID, bencher.deliveryEventCounter, messageID)
			}
			bencher.deliveryEventWg.Done()
			if bencher.deliveryEventCounter == bencher.payloadCount {
				bencher.timeEndDeliveryEvent = time.Now()
			}

		case "RECEIVED":
			if !stats.timeReceivedEvent.IsZero() {
				log.Printf("message %s event is already received\n", messageID)
				break
			}
			stats.timeReceivedEvent = time.Now()
			bencher.receivedEventCounter++
			bencher.messagesToSend <- bencher.payloadCount - bencher.outboxCounter
			if bencher.verbose {
				log.Printf("##### received  ack: %d,\t total: %d,\t msgID: %s\n", stats.ID, bencher.receivedEventCounter, messageID)
			}
			bencher.receiverEventWg.Done()
			if bencher.receivedEventCounter == bencher.payloadCount {
				bencher.timeEndReceiveEvent = time.Now()
			}

		default:
			log.Printf("unknown send event value: %s\n", msg.Value)
		}
	}
}

func (bencher *Bencher) calcFlightTime(printMissing bool) FlightTimeStatistics {
	return FlightTimeStatistics{
		DeliveryEvent: bencher.calcFlightTimeStats("DELIVERED_EVENT", printMissing),
		ReceivedEvent: bencher.calcFlightTimeStats("RECEIVED_EVENT", printMissing),
		Received:      bencher.calcFlightTimeStats("RECEIVED", printMissing),
		Outbox:        bencher.calcFlightTimeStats("CREATED", printMissing),
		Sent:          bencher.calcFlightTimeStats("ACCEPTED", printMissing),
	}
}

func (bencher *Bencher) calcFlightTimeStats(endStat string, printMissing bool) FlightTimeStats {
	var stats FlightTimeStats
	var total time.Duration
	flightTimes := make([]time.Duration, 0, bencher.payloadCount)
	for key, val := range bencher.messageStatsByBaMsgID {
		startTime := bencher.getTime(val, "CREATED")
		if startTime.IsZero() {
			if printMissing {
				log.Printf("never received %s for msg: %s", "CREATED", key)
			}
			continue
		}
		endTime := bencher.getTime(val, endStat)
		if endTime.IsZero() {
			if printMissing {
				log.Printf("never received %s for msg: %s", endStat, key)
			}
			continue
		}
		flightTime := endTime.Sub(startTime)
		flightTimes = append(flightTimes, flightTime)
		total += flightTime
	}

	count := len(flightTimes)
	if count > 0 {
		// Avg
		stats.Average = total / time.Duration(count)

		// Median
		sort.Slice(flightTimes, func(i, j int) bool { return flightTimes[i] < flightTimes[j] })
		mNumber := count / 2
		if count%2 != 0 {
			stats.Median = flightTimes[mNumber]
		} else {
			stats.Median = (flightTimes[mNumber-1] + flightTimes[mNumber]) / 2
		}
	}

	return stats
}

func (bencher *Bencher) getTime(stats *MessageStats, stat string) time.Time {
	switch stat {
	case "CREATED":
		return stats.timeCreated
	case "ACCEPTED":
		return stats.timeAccepted
	case "DELIVERED_EVENT":
		return stats.timeDeliveredEvent
	case "RECEIVED_EVENT":
		return stats.timeReceivedEvent
	case "RECEIVED":
		return stats.timeReceived
	}
	return time.Time{}
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
	flightTimes := bencher.calcFlightTime(printMissing)

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
		flightTimes.Sent.Average.Seconds(),
		flightTimes.Sent.Median.Seconds(),
	)
	log.Printf("Received in inbox: %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3f (avg) %.3f (median)\n",
		float64(bencher.receivedCounter)/receivedDuration.Seconds(),
		float64(bencher.receivedCounter)/receivedDuration.Seconds()*float64(bencher.payloadSize)/1000000,
		bencher.receivedCounter,
		bencher.payloadCount,
		receivedDuration.Round(time.Millisecond),
		flightTimes.Received.Average.Seconds(),
		flightTimes.Received.Median.Seconds(),
	)
	log.Printf("Delivery Event   : %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3fs (avg) %.3fs (median)\n",
		float64(bencher.deliveryEventCounter)/deliveryEventDuration.Seconds(),
		float64(bencher.deliveryEventCounter)/deliveryEventDuration.Seconds()*float64(estimatedAckSize)/1000000,
		bencher.deliveryEventCounter,
		bencher.payloadCount,
		deliveryEventDuration.Round(time.Millisecond),
		flightTimes.DeliveryEvent.Average.Seconds(),
		flightTimes.DeliveryEvent.Median.Seconds(),
	)
	log.Printf("Received Event   : %.3f msgs/s\t ~%.3f mb/s\t %d/%d msgs\t duration: %s\t flight time: %.3fs (avg) %.3fs (median)\n",
		float64(bencher.receivedEventCounter)/receivedEventDuration.Seconds(),
		float64(bencher.receivedEventCounter)/receivedEventDuration.Seconds()*float64(estimatedAckSize)/1000000,
		bencher.receivedEventCounter,
		bencher.payloadCount,
		receivedEventDuration.Round(time.Millisecond),
		flightTimes.Received.Average.Seconds(),
		flightTimes.Received.Median.Seconds(),
	)

	log.Printf("==============================================================================")
}

func (bencher *Bencher) close() {
	if bencher.outboxClient != nil {
		bencher.outboxClient.Close()
	}
	if bencher.outboxSender != nil {
		bencher.outboxSender.Close(context.Background())
	}

	if bencher.inboxClient != nil {
		bencher.inboxClient.Close()
	}
	if bencher.inboxReceiver != nil {
		bencher.inboxReceiver.Close(context.Background())
	}
}

func fillPayloadWithData(data []byte) {
	_, err := rand.Read(data)
	if err != nil {
		log.Fatal("Creating pseudo random payload:", err)
	}
}
