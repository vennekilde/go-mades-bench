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

	outboxReply       string
	outbox            string
	outboxSocketAddr  string
	outboxAmqpUser    string
	outboxAmqpPass    string
	outboxClient      *amqp.Client
	outboxSender      *amqp.Sender
	outboxAckReceiver *amqp.Receiver

	inbox           string
	inboxSocketAddr string
	inboxAmqpUser   string
	inboxAmqpPass   string
	inboxClient     *amqp.Client
	inboxReceiver   *amqp.Receiver

	senderWg     sync.WaitGroup
	receiverWg   sync.WaitGroup
	timeStart    time.Time
	timeEnd      time.Time
	counter      int32
	messageStats map[string]MessageStats
}

type MessageStats struct {
	timeSent     time.Time
	timeReceived time.Time
}

func main() {
	bencher := Bencher{}
	flag.IntVar(&bencher.payloadSize, "size", 1000000, "Incompressible payload size to generate") //1000000 = 1mb,
	flag.IntVar(&bencher.payloadCount, "n", 10000, "Number of messages to send")
	flag.IntVar(&bencher.maxInTransit, "max-in-transit", -1, "Max messages allowed in transit. max-in-transit < 0 means unlimited")
	flag.IntVar(&bencher.goroutines, "goroutines", runtime.NumCPU(), "Number of go routines to use when sending")
	flag.StringVar(&bencher.receiverCode, "receiver", "TEST-CODE", "Receiver Component Code")
	flag.StringVar(&bencher.messageType, "message-type", "TEST-MESSAGE", "Message type to send messages with")

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
	bencher.cleanInbox()
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
	bencher.outboxAckReceiver, err = recvSession.NewReceiver(
		amqp.LinkSourceAddress(bencher.outboxReply),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating outbox receiver link:", err)
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

func (bencher *Bencher) cleanInbox() {
	// Drain inbox before testing
	log.Println("Cleaning inbox before commencing test...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		msg, err := bencher.inboxReceiver.Receive(ctx)
		cancel()
		if err != nil {
			break
		}
		bencher.inboxReceiver.AcceptMessage(context.Background(), msg)
	}
}

func (bencher *Bencher) startBenching() {
	bencher.counter = int32(bencher.payloadCount)
	bencher.timeStart = time.Now()
	bencher.receiverWg.Add(bencher.payloadCount)

	log.Printf("Starting %s\n", bencher.timeStart.String())

	bencher.sendMessages()
	go bencher.receiveMessages()
	go bencher.receiveAcks()

	bencher.senderWg.Wait()
	bencher.receiverWg.Wait()
	bencher.timeEnd = time.Now()
}

func (bencher *Bencher) sendMessages() {
	for k := 0; k < bencher.goroutines; k++ {
		bencher.senderWg.Add(1)
		go func() {
			log.Printf("starting outbox worker #%d\n", k)
			defer bencher.senderWg.Done()

			for atomic.AddInt32(&bencher.counter, -1) >= 0 {
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
				bencher.messageStats[correlationID] = MessageStats{
					timeSent: time.Now(),
				}
				bencher.m.Unlock()
			}
			log.Printf("outbox worker #%d completed\n", k)
		}()
	}
}

func (bencher *Bencher) receiveMessages() {
	for i := 0; i < bencher.payloadCount; i++ {
		ctx := context.Background()
		// Receive next message
		msg, err := bencher.inboxReceiver.Receive(ctx)
		if err != nil {
			log.Fatal("Reading message from AMQP:", err)
		}

		// Accept message
		err = bencher.inboxReceiver.AcceptMessage(ctx, msg)
		if err != nil {
			log.Fatal("Accepting message from AMQP:", err)
		}
		bencher.receiverWg.Done()
		log.Printf("received %d\n", i)
	}
}

func (bencher *Bencher) receiveAcks() {
	for i := 0; i < bencher.payloadCount; i++ {
		ctx := context.Background()
		// Receive next message
		msg, err := bencher.inboxReceiver.Receive(ctx)
		if err != nil {
			log.Fatal("Reading message from AMQP:", err)
		}

		// Accept message
		err = bencher.inboxReceiver.AcceptMessage(ctx, msg)
		if err != nil {
			log.Fatal("Accepting message from AMQP:", err)
		}
		bencher.receiverWg.Done()
		log.Printf("received %d\n", i)
	}
}

func (bencher *Bencher) printBenchResults() {
	duration := bencher.timeEnd.Sub(bencher.timeStart)
	log.Printf("Started at       : %s\n", bencher.timeStart.String())
	log.Printf("Ended at         : %s\n", bencher.timeEnd.String())
	log.Printf("Duration         : %s\n", duration.String())
	log.Printf("Msg size         : %d bytes\n", bencher.payloadSize)
	log.Printf("Msg Throughput   : %f msgs/s\n", float64(bencher.payloadCount)/duration.Seconds())
	log.Printf("Data Throughput  : %f mb/s (estimated)\n", float64(bencher.payloadCount)/duration.Seconds()*float64(bencher.payloadSize)/1000000)
	log.Printf("Msg transit time : %d s (avg)\n", -1)
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
