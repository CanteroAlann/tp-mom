package factory

import (
	"context"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQQueue struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
	tag       string
	consuming bool
	stopChan  chan struct{}
}

func NewRabbitMQQueue(queueName string, settings m.ConnSettings) (*RabbitMQQueue, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", settings.Hostname, settings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, ErrRabbitMQCreateQueueMiddleware
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, ErrRabbitMQCreateChannel
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
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
		ch:        ch,
		queueName: queueName,
		tag:       fmt.Sprintf("consumer-%s", queueName),
		stopChan:  make(chan struct{}),
	}, nil
}

func (r *RabbitMQQueue) Send(msg m.Message) error {

	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := r.ch.PublishWithContext(
		ctx,
		"",
		r.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(msg.Body),
		},
	)
	if err != nil {
		if r.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (r *RabbitMQQueue) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	if r.consuming {
		return nil
	}

	deliveries, err := r.ch.Consume(
		r.queueName,
		r.tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	r.consuming = true
	r.stopChan = make(chan struct{})

	for {
		select {
		case <-r.stopChan:
			return nil
		case d, ok := <-deliveries:
			if !ok {
				r.consuming = false
				return m.ErrMessageMiddlewareDisconnected
			}

			ack := func() { _ = d.Ack(false) }
			nack := func() { _ = d.Nack(false, true) }

			callbackFunc(m.Message{Body: string(d.Body)}, ack, nack)
		}
	}
}

func (r *RabbitMQQueue) StopConsuming() error {
	if !r.consuming {
		return nil
	}

	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err := r.ch.Cancel(r.tag, false)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	close(r.stopChan)
	r.consuming = false
	return nil
}

func (r *RabbitMQQueue) Close() error {
	var err error
	if r.ch != nil && !r.ch.IsClosed() {
		if chErr := r.ch.Close(); chErr != nil {
			err = chErr
		}
	}
	if r.conn != nil && !r.conn.IsClosed() {
		if connErr := r.conn.Close(); connErr != nil {
			err = connErr
		}
	}

	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
