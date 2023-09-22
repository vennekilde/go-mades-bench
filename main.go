package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)

	bencher := NewBencher()

	// Usage cli
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
	}

	// Initialize using flag values
	flag.BoolVar(&bencher.verbose, "v", false, "Print all message events")

	flag.Uint64Var(&bencher.payloadSize, "size", 1000000, "Incompressible payload size to generate") //1000000 = 1mb,
	flag.Uint64Var(&bencher.payloadCount, "n", 10000, "Number of messages to send")
	flag.Uint64Var(&bencher.maxInTransit, "max-in-transit", 1000, "Max messages allowed in transit. max-in-transit <= 0 means unlimited")
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

	var mode string
	flag.StringVar(&mode, "mode", "endpoint", "mode")

	flag.Parse()

	// Print used flags
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("-%s=%s ", f.Name, f.Value)
	})
	fmt.Print("\n")

	defer bencher.close()

	switch mode {
	case "endpoint":
		bencher.ConfigureForEndpoint()
	case "tracing":
		bencher.ConfigureForEndpointTracing()
	case "amqp":
		bencher.ConfigureForAMQP()
	case "toolbox":
		bencher.ConfigureForToolbox()
	default:
		zap.L().Panic("unknown mode", zap.String("mode", mode))
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		bencher.startBenching()
		// Completed
		interrupt <- syscall.Signal(0x0)
	}()

	killSignal := <-interrupt
	switch killSignal {
	case os.Interrupt:
		fmt.Println("Received OS Interrupt")
	case syscall.SIGTERM:
		fmt.Println("Received Termination Signal")
	}

	//bencher.printMissing()

	log.Printf("\n")
	log.Print("Ran with flags: ")
	// Print used flags
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("-%s=%s ", f.Name, f.Value)
	})

	fmt.Print("\n")
	log.Printf("Final Report\n")
	bencher.tracker.BenchResults(os.Stdout)
}
