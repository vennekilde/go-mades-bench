package main

import (
	"context"
	"crypto/tls"
	"flag"
	"log"
	"math/rand"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

var client *amqp.Client

func main() {
	payloadSize := flag.Int("size", 1000000, "an int") //1000000 = 1mb
	outboxSocketAddr := flag.String("outbox-addr", "amqp://localhost:5672", "Socket address to reach the internal broker")
	inboxSocketAddr := flag.String("inbox-addr", "amqp://localhost:5672", "Socket address to reach the internal broker")
	outboxAmqpUser := flag.String("outbox-user", "admin", "an int")
	outboxAmqpPass := flag.String("outbox-pass", "admin", "an int")
	inboxAmqpUser := flag.String("inbox-user", "admin", "an int")
	inboxAmqpPass := flag.String("inbox-pass", "admin", "an int")
	payloadCount := flag.Int("n", 10000, "an int")
	//measureFrom := flag.Int("n", 0, "an int")
	//measureTo := flag.Int("n", 0, "an int")
	outbox := flag.String("outbox", "mades.endpoint.outbox", "an int")
	//outboxReply := flag.String("outbox-reply", "mades.endpoint.outbox.reply", "an int")
	inbox := flag.String("inbox", "mades.endpoint.inbox", "an int")
	receiverCode := flag.String("receiver", "TEST-CODE", "an int")
	messageType := flag.String("message-type", "TEST-MESSAGE", "an int")
	goroutines := flag.Int("goroutines", runtime.NumCPU(), "an int")
	flag.Parse()

	var counter int32 = int32(*payloadCount)

	// Create outboxClient
	outboxClientOpts := []amqp.ConnOption{
		amqp.ConnSASLPlain(*outboxAmqpUser, *outboxAmqpPass),
	}
	if strings.HasPrefix(*outboxSocketAddr, "amqps://") {
		outboxClientOpts = append(outboxClientOpts, amqp.ConnTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}

	outboxClient, err := amqp.Dial(*outboxSocketAddr, outboxClientOpts...)
	if err != nil {
		log.Fatal("Dialing AMQP outbox server:", err)
	}
	defer outboxClient.Close()

	sendSession, err := outboxClient.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP receive session:", err)
	}
	// Create a sender
	sender, err := sendSession.NewSender(
		amqp.LinkTargetAddress(*outbox),
	)
	if err != nil {
		log.Fatal("Creating sender link:", err)
	}
	defer sender.Close(context.Background())

	// Create inboxClient
	inboxClientOpts := []amqp.ConnOption{
		amqp.ConnSASLPlain(*inboxAmqpUser, *inboxAmqpPass),
	}
	if strings.HasPrefix(*inboxSocketAddr, "amqps://") {
		inboxClientOpts = append(inboxClientOpts, amqp.ConnTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}

	inboxClient, err := amqp.Dial(*inboxSocketAddr, inboxClientOpts...)
	if err != nil {
		log.Fatal("Dialing AMQP inbox server:", err)
	}
	defer inboxClient.Close()

	recvSession, err := inboxClient.NewSession()
	if err != nil {
		log.Fatal("Creating AMQP receive session:", err)
	}

	// Create a receiver
	receiver, err := recvSession.NewReceiver(
		amqp.LinkSourceAddress(*inbox),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating receiver link:", err)
	}
	defer receiver.Close(context.Background())

	// Drain inbox before testing
	log.Println("Cleaning inbox before commencing test...")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		msg, err := receiver.Receive(ctx)
		cancel()
		if err != nil {
			break
		}
		receiver.AcceptMessage(context.Background(), msg)
	}

	timeStart := time.Now()
	log.Printf("Starting %s\n", timeStart.String())
	var senderWg sync.WaitGroup
	var receiverWg sync.WaitGroup
	for k := 0; k < *goroutines; k++ {
		senderWg.Add(1)
		go func() {
			defer senderWg.Done()

			for atomic.AddInt32(&counter, -1) >= 0 {
				payload := make([]byte, *payloadSize)
				fillPayloadWithData(payload)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
				msg := &amqp.Message{
					Data: [][]byte{payload},
					ApplicationProperties: map[string]interface{}{
						"receiverCode":      *receiverCode,
						"messageType":       *messageType,
						"baMessageID":       uuid.New().String(),
						"senderApplication": "Go-MADES-Bench",
					},
					Properties: &amqp.MessageProperties{CreationTime: &timeStart},
				}
				err := sender.Send(ctx, msg)
				if err != nil {
					log.Fatal("Sending message:", err)
				}
				cancel()
			}
		}()
	}

	receiverWg.Add(*payloadCount)
	recvCount := 0
	go func() {
		for i := 0; i < *payloadCount; i++ {
			ctx := context.Background()
			// Receive next message
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Fatal("Reading message from AMQP:", err)
			}

			// Accept message
			err = receiver.AcceptMessage(ctx, msg)
			if err != nil {
				log.Fatal("Accepting message from AMQP:", err)
			}
			receiverWg.Done()
			recvCount++
			log.Printf("received %d\n", recvCount)
		}
	}()
	senderWg.Wait()
	receiverWg.Wait()
	timeEnd := time.Now()
	duration := timeEnd.Sub(timeStart)
	log.Printf("Started at : %s\n", timeStart.String())
	log.Printf("Ended at   : %s\n", timeEnd.String())
	log.Printf("Duration   : %s\n", duration.String())
	log.Printf("Throughput : %f msgs/s\n", float64(*payloadCount)/duration.Seconds())
	log.Printf("Msg size   : %d bytes\n", *payloadSize)
}

func fillPayloadWithData(data []byte) {
	_, err := rand.Read(data)
	if err != nil {
		log.Fatal("Creating pseudo random payload:", err)
	}
}
