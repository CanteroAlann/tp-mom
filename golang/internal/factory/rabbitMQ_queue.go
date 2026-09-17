package factory

import (
	"context"
	"fmt"
	"sync"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueue struct {
	conn      *RabbitMQConnection
	queueName string
	tag       string
	consuming bool
	stopChan  chan struct{}
	lock      sync.Mutex
}

func NewRabbitMQQueue(queueName string, settings m.ConnSettings) (*RabbitMQQueue, error) {
	conn, err := NewRabbitMQConnection(settings.Hostname, settings.Port)
	if err != nil {
		return nil, err
	}

	ch := conn.GetChannel()

	_, err = ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, ErrRabbitMQDeclareQueue
	}

	return &RabbitMQQueue{
		conn:      conn,
		queueName: queueName,
		tag:       fmt.Sprintf("consumer-%s", queueName),
		stopChan:  make(chan struct{}),
	}, nil
}

func (r *RabbitMQQueue) Send(msg m.Message) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := r.conn.GetChannel()

	err := ch.PublishWithContext(
		ctx,
		"",
		r.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		},
	)
	if err != nil {
		if err == amqp.ErrClosed {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (r *RabbitMQQueue) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	r.lock.Lock()
	if r.conn.IsClosed() {
		r.lock.Unlock()
		return m.ErrMessageMiddlewareDisconnected
	}
	if r.consuming {
		r.lock.Unlock()
		return nil
	}
	r.consuming = true
	r.stopChan = make(chan struct{})
	r.lock.Unlock()

	ch := r.conn.GetChannel()
	deliveries, err := ch.Consume(
		r.queueName,
		r.tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if err == amqp.ErrClosed {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	for {
		select {
		case <-r.stopChan:
			return nil
		case d, ok := <-deliveries:
			if !ok {
				if r.conn.IsClosed() {
					return m.ErrMessageMiddlewareDisconnected
				}
				return m.ErrMessageMiddlewareMessage
			}
			ack := func() { _ = d.Ack(false) }
			nack := func() { _ = d.Nack(false, true) }

			callbackFunc(m.Message{Body: string(d.Body)}, ack, nack)
		}
	}
}

func (r *RabbitMQQueue) StopConsuming() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !r.consuming {
		return nil
	}

	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err := r.conn.StopConsuming(r.tag)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	close(r.stopChan)
	r.consuming = false
	return nil
}

func (r *RabbitMQQueue) Close() error {
	r.lock.Lock()
	if r.consuming {
		r.consuming = false
		close(r.stopChan)
	}
	r.lock.Unlock()
	err := r.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
