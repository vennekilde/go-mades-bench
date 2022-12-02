package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
)

func cleanQueues(conn *AMQPConn) {
	cleanReceivers(conn.receivers...)
}

func cleanReceivers(receivers ...*amqp.Receiver) {
	wg := sync.WaitGroup{}
	wg.Add(len(receivers))
	for _, receiver := range receivers {
		go cleanQueue(&wg, receiver)
	}
	wg.Wait()
}

func cleanQueue(wg *sync.WaitGroup, receiver *amqp.Receiver) {
	// Drain  outbox reply before testing
	log.Printf("Cleaning amqp queue %s ...\n", receiver.Address())
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
