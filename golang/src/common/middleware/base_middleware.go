package middleware

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	consumerTag string
}

func newBaseMiddleware(connectionSettings ConnSettings) (*baseMiddleware, error) {
	bm := new(baseMiddleware)

	addr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	var err error

	bm.conn, err = amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	bm.ch, err = bm.conn.Channel()
	if err != nil {
		bm.conn.Close()
		return nil, err
	}

	bm.consumerTag = ""

	return bm, nil
}

func (bm *baseMiddleware) isDisconnected() bool {
	return bm.conn == nil || bm.conn.IsClosed() || bm.ch == nil
}

func (bm *baseMiddleware) stop() error {
	if bm.consumerTag == "" {
		return nil
	}
	tag := bm.consumerTag
	bm.consumerTag = ""

	if bm.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}
	err := bm.ch.Cancel(tag, false)

	if err != nil {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (bm *baseMiddleware) close() error {
	var errCh, errConn error
	if bm.ch != nil {
		errCh = bm.ch.Close()
	}
	if bm.conn != nil {
		errConn = bm.conn.Close()
	}
	bm.ch, bm.conn, bm.consumerTag = nil, nil, ""
	if errCh != nil || errConn != nil {
		return ErrMessageMiddlewareClose
	}
	return nil
}

func (bm *baseMiddleware) consume(queueName string, callBackFunc func(msg Message, ack func(), nack func())) error {
	bm.consumerTag = fmt.Sprintf("consumer%d", time.Now().UnixNano())
	msgs, err := bm.ch.Consume(
		queueName,
		bm.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := Message{Body: string(d.Body)}

		ack := func() {
			d.Ack(
				false, // only ack this message, not the ones before
			)
		}
		nack := func() {
			d.Nack(
				false, // only nack this message, not the ones before
				true,  // requeue this message instead of discarding it
			)
		}
		callBackFunc(msg, ack, nack)
	}

	return ErrMessageMiddlewareMessage // msgs is closed
}

func (bm *baseMiddleware) publish(msg Message, ctx context.Context, exchange string, key string) error {
	return bm.ch.PublishWithContext(
		ctx,
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // survives broker restarts
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})
}
