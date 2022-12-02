package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"strings"

	"github.com/Azure/go-amqp"
)

type AMQPConnOpts struct {
	socketAddr string

	sendQueues []string
	recvQueues []string

	username string
	password string
}

type AMQPConn struct {
	client    *amqp.Client
	senders   []*amqp.Sender
	receivers []*amqp.Receiver
}

func newAMQPConn(opts *AMQPConnOpts) (conn *AMQPConn, err error) {
	conn = &AMQPConn{}

	// Create outboxClient
	clientOpts := []amqp.ConnOption{
		amqp.ConnSASLPlain(opts.username, opts.password),
	}
	if strings.HasPrefix(opts.socketAddr, "amqps://") {
		// #nosec G402 - Ignore InsecureSkipVerify: true. This is a benchmark tool, no need to enforce strict security checking
		clientOpts = append(clientOpts, amqp.ConnTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}))
	}

	conn.client, err = amqp.Dial(opts.socketAddr, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("Dialing AMQP server: %w", err)
	}

	if len(opts.sendQueues) > 0 {
		sendSession, err := conn.client.NewSession()
		if err != nil {
			return nil, fmt.Errorf("Creating AMQP sender session: %w", err)
		}

		for _, queue := range opts.sendQueues {
			// Create outbox sender
			sender, err := sendSession.NewSender(
				amqp.LinkTargetAddress(queue),
			)
			if err != nil {
				return nil, fmt.Errorf("Creating sender link: %w", err)
			}
			conn.senders = append(conn.senders, sender)
		}
	}

	if len(opts.recvQueues) > 0 {
		recvSession, err := conn.client.NewSession()
		if err != nil {
			return nil, fmt.Errorf("Creating AMQP outbox receive session: %w", err)
		}

		for _, queue := range opts.recvQueues {
			// Create outbox reply receiver
			receiver, err := recvSession.NewReceiver(
				amqp.LinkSourceAddress(queue),
				amqp.LinkCredit(10),
			)
			if err != nil {
				return nil, fmt.Errorf("Creating receiver link: %w", err)
			}
			conn.receivers = append(conn.receivers, receiver)
		}
	}

	return conn, nil
}

func (conn *AMQPConn) Close() error {
	for _, sender := range conn.senders {
		err := sender.Close(context.Background())
		if err != nil {
			log.Printf("unable to close sender amqp connection. err: %s\n", err.Error())
		}
	}
	for _, receiver := range conn.receivers {
		err := receiver.Close(context.Background())
		if err != nil {
			log.Printf("unable to close receiver amqp connection. err: %s\n", err.Error())
		}
	}

	return conn.client.Close()
}
