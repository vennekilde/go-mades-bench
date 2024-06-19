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
	conn      *amqp.Conn
	senders   []*amqp.Sender
	receivers []*amqp.Receiver
}

func newAMQPConn(opts *AMQPConnOpts) (conn *AMQPConn, err error) {
	ctx := context.Background()
	conn = &AMQPConn{}

	// Create outboxClient
	connOpts := amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain(opts.username, opts.password),
	}
	if strings.HasPrefix(opts.socketAddr, "amqps://") {
		// #nosec G402 - Ignore InsecureSkipVerify: true. This is a benchmark tool, no need to enforce strict security checking
		connOpts.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	conn.conn, err = amqp.Dial(ctx, opts.socketAddr, &connOpts)
	if err != nil {
		return nil, fmt.Errorf("Dialing AMQP server: %w", err)
	}

	if len(opts.sendQueues) > 0 {
		sendSession, err := conn.conn.NewSession(context.Background(), &amqp.SessionOptions{})
		if err != nil {
			return nil, fmt.Errorf("Creating AMQP sender session: %w", err)
		}

		for _, queue := range opts.sendQueues {
			// Create outbox sender
			sender, err := sendSession.NewSender(
				context.Background(),
				queue,
				&amqp.SenderOptions{
					Durability:       amqp.DurabilityUnsettledState,
					TargetDurability: amqp.DurabilityUnsettledState,
					TargetCapabilities: []string{
						// QPID broker capabilities
						"create-on-demand",
						"durable",
						"shared",
						// Artemis capabilities
						"queue",
					},
				},
			)
			if err != nil {
				return nil, fmt.Errorf("Creating sender link: %w", err)
			}
			conn.senders = append(conn.senders, sender)
		}
	}

	if len(opts.recvQueues) > 0 {
		recvSession, err := conn.conn.NewSession(context.Background(), &amqp.SessionOptions{})
		if err != nil {
			return nil, fmt.Errorf("Creating AMQP outbox receive session: %w", err)
		}

		for _, queue := range opts.recvQueues {
			// Create outbox reply receiver
			receiver, err := recvSession.NewReceiver(
				context.Background(),
				queue,
				&amqp.ReceiverOptions{
					Credit:           1000,
					Durability:       amqp.DurabilityUnsettledState,
					SourceDurability: amqp.DurabilityUnsettledState,
					SourceCapabilities: []string{
						// QPID broker capabilities
						"create-on-demand",
						"durable",
						"shared",
						// Artemis capabilities
						"queue",
					},
				},
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

	return conn.conn.Close()
}
