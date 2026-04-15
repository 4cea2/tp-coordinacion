package middleware

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	*baseMiddleware
	q amqp.Queue
}

func NewQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	qm := new(QueueMiddleware)
	base, err := newBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}
	qm.baseMiddleware = base
	qm.q, err = qm.ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		qm.close()
		return nil, err
	}

	err = qm.ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)

	if err != nil {
		qm.close()
		return nil, err
	}
	return qm, nil
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if qm.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}

	return qm.consume(qm.q.Name, callbackFunc)
}

func (qm *QueueMiddleware) StopConsuming() error {
	return qm.stop()
}

func (qm *QueueMiddleware) Send(msg Message) error {
	if qm.isDisconnected() {
		return ErrMessageMiddlewareDisconnected
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // cancel the publish operation if it takes longer than 5 seconds
	defer cancel()

	errPublish := qm.publish(msg, ctx, "", qm.q.Name)
	if errPublish != nil {
		return ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return qm.close()
}
