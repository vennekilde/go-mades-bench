package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Queues struct {
	SenderQueue   *SenderQueue
	ReceiveQueues []*ReceiveQueue
}

func (bq *Queues) Start() {
	for _, queue := range bq.ReceiveQueues {
		go queue.Start()
	}

	go bq.SenderQueue.Start()
}

type Queue struct {
	Trackables     []*Trackable
	listener       MessageEventListener
	listenerType   int
	MessageTracker *MessageTracker
}

func NewQueue(msgTracker *MessageTracker, trackables ...*Trackable) Queue {
	return Queue{
		Trackables:     trackables,
		MessageTracker: msgTracker,
	}
}

func (q *Queue) onMessage(trackableType int, msg *MessageIdent) {
	if msg != nil {
		if q.listener != nil && q.listenerType == trackableType {
			q.listener <- msg.ID
		}
		dur := time.Since(msg.Created)
		q.Trackables[trackableType].Add(dur)
	}
}

type MsgCreator = func(id uint64, size uint64) (*amqp.Message, string, *MessageIdent)
type MessageEventListener chan uint64

type SenderQueue struct {
	Queue
	PayloadSize uint64
	CreateMsg   MsgCreator

	sender      *amqp.Sender
	senderCount int
	sendChan    MessageEventListener

	maxUnacknowledged uint64
}

func (s *SenderQueue) Start() {
	var n atomic.Uint64
	limit := s.Queue.Trackables[0].Limit

	for i := 0; i < s.senderCount; i++ {
		go func() {
			for {
				<-s.sendChan
				id := n.Add(1)
				if id > limit && limit > 0 {
					for {
						// empty queue
						<-s.sendChan
					}
				}
				s.Send(id)
			}
		}()
	}

	if s.maxUnacknowledged > 0 {
		toSendNow := s.maxUnacknowledged
		if limit > 0 && toSendNow > limit {
			toSendNow = limit
		}
		for i := uint64(0); i < toSendNow; i++ {
			s.sendChan <- i
		}
	} else {
		for i := uint64(0); i < limit; i++ {
			s.sendChan <- i
		}
	}
}

func (s *SenderQueue) Send(id uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	msg, baMsgId, msgIdent := s.CreateMsg(id, s.PayloadSize)
	s.MessageTracker.Add(baMsgId, msgIdent)
	err := s.sender.Send(ctx, msg, nil)
	if err != nil {
		panic(err)
	}
	s.Queue.onMessage(0, msgIdent)
}

func CreateMadesMsgCreator(receiverCode string, messageType string, isTracing bool) MsgCreator {
	return func(id uint64, size uint64) (*amqp.Message, string, *MessageIdent) {
		payload := make([]byte, size)
		fillPayloadWithData(payload)

		creationTime := time.Now()
		baMessageID := fmt.Sprintf("%d-%s", id, uuid.New().String())
		msg := &amqp.Message{
			Data: [][]byte{payload},
			ApplicationProperties: map[string]interface{}{
				"receiverCode":      receiverCode,
				"messageType":       messageType,
				"baMessageID":       baMessageID,
				"isTracingMessage":  isTracing,
				"senderApplication": "Go-MADES-Bench",
			},
			Properties: &amqp.MessageProperties{
				CreationTime:  &creationTime,
				CorrelationID: baMessageID,
			},
			Header: &amqp.MessageHeader{
				Durable: true,
			},
		}

		msgIdent := &MessageIdent{
			ID:      id,
			Created: creationTime,
		}

		return msg, baMessageID, msgIdent
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

type ReceiveQueue struct {
	Queue
	IdentifyMsg func(messageTracker *MessageTracker, msg *amqp.Message) (*MessageIdent, string, int)
	receiver    *amqp.Receiver
}

func (r *ReceiveQueue) Start() {
	ctx := context.Background()
	for {
		// Receive next message
		msg, err := r.receiver.Receive(ctx, nil)
		if err != nil {
			if r.Trackables[0].Completed {
				break
			}
			zap.L().Fatal("Reading message from AMQP", zap.Error(err))
		}
		r.ReceiveMessage(msg)
		// Accept message
		err = r.receiver.AcceptMessage(ctx, msg)
		if err != nil {
			log.Fatal("Accepting message from AMQP:", err)
		}
	}
}

func (r *ReceiveQueue) ReceiveMessage(msg *amqp.Message) {
	msgIdent, msgID, trackableType := r.IdentifyMsg(r.Queue.MessageTracker, msg)
	if msgIdent == nil {
		zap.L().Warn("unknown message", zap.Any("msg", msg))
	}
	r.Queue.onMessage(trackableType, msgIdent)

	if msg.ApplicationProperties["errorCode"] != nil {
		zap.L().Error("received message error code",
			zap.String("msgID", msgID),
			zap.Any("msgIdent", msgIdent),
			zap.Any("ApplicationProperties", msg.ApplicationProperties))
	}
}

func identifyMsgByCorrelationID(messageTracker *MessageTracker, msg *amqp.Message) (*MessageIdent, string) {
	// Correlation id is used for outbox.reply events
	if msg.Properties == nil || msg.Properties.CorrelationID == nil {
		return nil, ""
	}

	correlationID := msg.Properties.CorrelationID.(string)
	msgIdent := messageTracker.Get(correlationID)

	return msgIdent, correlationID
}

func identifyMsgByBaMsgID(messageTracker *MessageTracker, msg *amqp.Message) (*MessageIdent, string) {
	baMsgID, ok := msg.ApplicationProperties["baMessageID"].(string)
	if !ok {
		return nil, ""
	}
	msgIdent := messageTracker.Get(baMsgID)

	return msgIdent, baMsgID
}

func HandleMadesReplyMessage(messageTracker *MessageTracker, msg *amqp.Message) (*MessageIdent, string, int) {
	msgIdent, msgID := identifyMsgByCorrelationID(messageTracker, msg)
	return msgIdent, msgID, 0
}

func HandleMadesInboxMessage(messageTracker *MessageTracker, msg *amqp.Message) (*MessageIdent, string, int) {
	msgIdent, msgID := identifyMsgByBaMsgID(messageTracker, msg)
	return msgIdent, msgID, 0
}

func HandleMadesSendEventMessage(messageTracker *MessageTracker, msg *amqp.Message) (*MessageIdent, string, int) {
	msgIdent, msgID := identifyMsgByBaMsgID(messageTracker, msg)
	if msg.Value != nil {
		switch msg.Value.(string) {
		case "DELIVERED":
			return msgIdent, msgID, 0
		case "RECEIVED":
			return msgIdent, msgID, 1
		case "TRACING":
			return msgIdent, msgID, 0
		default:
			zap.L().Warn("unknown send event value",
				zap.String("msgID", msgID),
				zap.Any("value", msg.Value))
		}
	}
	return nil, "", 0
}
