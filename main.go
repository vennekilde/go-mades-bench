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

	payloadSize  int
	payloadCount int
	receiverCode string
	messageType  string
	goroutines   int
	maxInTransit int

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

	senderWg       sync.WaitGroup
	receiverWg     sync.WaitGroup
	timeStart      time.Time
	timeEnd        time.Time
	messagesToSend chan int32
	counter        int32
	messageStats   map[string]*MessageStats
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
	flag.IntVar(&bencher.payloadSize, "size", 1000000, "Incompressible payload size to generate") //1000000 = 1mb,
	flag.IntVar(&bencher.payloadCount, "n", 10000, "Number of messages to send")
	flag.IntVar(&bencher.maxInTransit, "max-in-transit", 0, "Max messages allowed in transit. max-in-transit <= 0 means unlimited")
	flag.IntVar(&bencher.goroutines, "goroutines", runtime.NumCPU(), "Number of go routines to use when sending")
	flag.StringVar(&bencher.receiverCode, "receiver", "TEST-CODE", "Receiver Component Code")
	flag.StringVar(&bencher.messageType, "message-type", "TEST-MESSAGE", "Message type to send messages with")

	flag.StringVar(&bencher.sendEvent, "send-event", "ecp.endpoint.send.event", "send event queue")
	flag.StringVar(&bencher.outboxReply, "outbox-reply", "ecp.endpoint.outbox.reply", "outbox reply queue")
	flag.StringVar(&bencher.outbox, "outbox", "ecp.endpoint.outbox", "outbox queue")
	flag.StringVar(&bencher.outboxSocketAddr, "outbox-addr", "amqp://localhost:5672", "Socket address to reach the internal broker")
	flag.StringVar(&bencher.outboxAmqpUser, "outbox-user", "admin", "Outbox broker username")
	flag.StringVar(&bencher.outboxAmqpPass, "outbox-pass", "password", "Outbox broker password")

	flag.StringVar(&bencher.inbox, "inbox", "ecp.endpoint.inbox", "inbox queue")
	flag.StringVar(&bencher.inboxSocketAddr, "inbox-addr", "amqp://localhost:5672", "Socket address to reach the internal broker")
	flag.StringVar(&bencher.inboxAmqpUser, "inbox-user", "admin", "Inbox broker password")
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
	bencher.printBenchResults()
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
	var i uint
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
	bencher.counter = int32(bencher.payloadCount)
	bencher.timeStart = time.Now()
	bencher.receiverWg.Add(bencher.payloadCount)
	bencher.messageStats = map[string]*MessageStats{}

	log.Printf("Starting %s\n", bencher.timeStart.String())

	bencher.sendMessages()
	go bencher.receiveMessages(bencher.outboxReplyReceiver)
	go bencher.receiveMessages(bencher.inboxReceiver)
	go bencher.receiveMessages(bencher.sendEventReceiver)

	bencher.senderWg.Wait()
	bencher.receiverWg.Wait()
	bencher.timeEnd = time.Now()

	log.Println("waiting a second for any out of order messages")
	time.Sleep(time.Second)
}

func (bencher *Bencher) sendMessages() {
	initialMessagesToSend := bencher.payloadCount
	if bencher.maxInTransit > 0 {
		initialMessagesToSend = bencher.maxInTransit
	}
	bencher.messagesToSend = make(chan int32, initialMessagesToSend)
	for i := 1; i <= initialMessagesToSend; i++ {
		bencher.messagesToSend <- int32(i)
	}
	for k := 0; k < bencher.goroutines; k++ {
		workerID := k
		bencher.senderWg.Add(1)
		go func() {
			log.Printf("starting outbox worker #%d\n", workerID)
			defer bencher.senderWg.Done()

			for atomic.AddInt32(&bencher.counter, -1) >= 0 {
				// Await for message to send
				n := <-bencher.messagesToSend

				payload := make([]byte, bencher.payloadSize)
				fillPayloadWithData(payload)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
				creationTime := time.Now()
				correlationID := uuid.New().String()
				msg := &amqp.Message{
					Data: [][]byte{payload},
					ApplicationProperties: map[string]interface{}{
						"receiverCode":      bencher.receiverCode,
						"messageType":       bencher.messageType,
						"baMessageID":       uuid.New().String(),
						"senderApplication": "Go-MADES-Bench",
					},
					Properties: &amqp.MessageProperties{
						CreationTime:  &creationTime,
						CorrelationID: correlationID,
					},
				}
				// Send message to outbox
				err := bencher.outboxSender.Send(ctx, msg)
				if err != nil {
					log.Fatal("Sending message:", err)
				}
				cancel()

				bencher.m.Lock()
				bencher.messageStats[correlationID] = &MessageStats{
					ID:          n,
					timeCreated: time.Now(),
				}
				bencher.m.Unlock()
				log.Printf("created %d\n", n)
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

func (bencher *Bencher) handleMessage(msg *amqp.Message, queue string) {
	var stats *MessageStats
	var correlationID, messageID string
	var ok bool

	bencher.m.Lock()
	if msg.Properties != nil && msg.Properties.CorrelationID != nil {
		correlationID = msg.Properties.CorrelationID.(string)
		stats = bencher.messageStats[correlationID]
	}
	if msg.ApplicationProperties != nil {
		if messageID, ok = msg.ApplicationProperties["messageID"].(string); ok {
			msgStats, ok := bencher.messageStats[messageID]
			if !ok {
				msgStats = &MessageStats{}
				bencher.messageStats[messageID] = msgStats
			}
			if stats != nil {
				msgStats.ID = stats.ID
				msgStats.timeCreated = stats.timeCreated
				msgStats.timeAccepted = stats.timeAccepted
				delete(bencher.messageStats, correlationID)
			}
			stats = msgStats
		}
	}
	bencher.m.Unlock()

	if stats == nil {
		log.Printf("unknown amqp message %#v", msg.ApplicationProperties)
		return
	}

	switch queue {
	case bencher.outboxReply:
		stats.timeAccepted = time.Now()
		log.Printf("sent %d\n", stats.ID)
	case bencher.inbox:
		stats.timeReceived = time.Now()
		log.Printf("received %d\n", stats.ID)
	case bencher.sendEvent:
		switch msg.Value.(string) {
		case "DELIVERED":
			stats.timeDeliveredEvent = time.Now()
			log.Printf("delivered ack %d\n", stats.ID)
		case "RECEIVED":
			if !stats.timeReceivedEvent.IsZero() {
				log.Printf("message %s is already received\n", messageID)
				break
			}
			stats.timeReceivedEvent = time.Now()
			bencher.receiverWg.Done()
			bencher.messagesToSend <- int32(bencher.payloadCount) - bencher.counter
			log.Printf("received ack %d\n", stats.ID)
		default:
			log.Printf("unknown send event value: %s\n", msg.Value)
		}
	}
}

func (bencher *Bencher) calcFlightTime() FlightTimeStatistics {
	return FlightTimeStatistics{
		DeliveryEvent: bencher.calcFlightTimeStats("DELIVERED_EVENT"),
		ReceivedEvent: bencher.calcFlightTimeStats("RECEIVED_EVENT"),
		Received:      bencher.calcFlightTimeStats("RECEIVED"),
	}
}

func (bencher *Bencher) calcFlightTimeStats(endStat string) FlightTimeStats {
	var stats FlightTimeStats
	var total time.Duration
	flightTimes := make([]time.Duration, 0, bencher.payloadCount)
	for key, val := range bencher.messageStats {
		startTime := bencher.getTime(val, "CREATED")
		if startTime.IsZero() {
			log.Printf("never received %s for msg: %s", "CREATED", key)
			continue
		}
		endTime := bencher.getTime(val, endStat)
		if endTime.IsZero() {
			log.Printf("never received %s for msg: %s", endStat, key)
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

func (bencher *Bencher) printBenchResults() {
	duration := bencher.timeEnd.Sub(bencher.timeStart)
	flightTimes := bencher.calcFlightTime()
	log.Printf("Started at       : %s\n", bencher.timeStart.String())
	log.Printf("Ended at         : %s\n", bencher.timeEnd.String())
	log.Printf("Duration         : %s\n", duration.String())
	log.Printf("Msg count        : %d \n", bencher.payloadCount)
	log.Printf("Msg size         : %d bytes\n", bencher.payloadSize)
	log.Printf("Msg Throughput   : %f msgs/s\n", float64(bencher.payloadCount)/duration.Seconds())
	log.Printf("Data Throughput  : %f mb/s (estimated)\n", float64(bencher.payloadCount)/duration.Seconds()*float64(bencher.payloadSize)/1000000)
	log.Printf("Msg transit time : %s (avg) %s (median) for Received\n", flightTimes.Received.Average.String(), flightTimes.Received.Median.String())
	log.Printf("Msg transit time : %s (avg) %s (median) for Delivery Event\n", flightTimes.DeliveryEvent.Average.String(), flightTimes.DeliveryEvent.Median.String())
	log.Printf("Msg transit time : %s (avg) %s (median) fpr Received Event\n", flightTimes.ReceivedEvent.Average.String(), flightTimes.ReceivedEvent.Median.String())
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
