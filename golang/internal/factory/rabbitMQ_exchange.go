package factory

import (
	"context"
	"fmt"
	"sync"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQExchange struct {
	conn      *RabbitMQConnection
	exchange  string
	keys      []string
	queueName string
	tag       string
	consuming bool
	mu        sync.Mutex
	stopChan  chan struct{}
}

func NewRabbitMQExchange(exchange string, keys []string, settings m.ConnSettings) (*RabbitMQExchange, error) {
	conn, err := NewRabbitMQConnection(settings.Hostname, settings.Port)
	if err != nil {
		return nil, err
	}

	ch := conn.GetChannel()

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		false,    // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		conn.Close()
		return nil, ErrRabbitMQDeclareExchange
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		conn.Close()
		return nil, ErrRabbitMQCreateExchangeMiddleware
	}

	for _, key := range keys {
		err = ch.QueueBind(
			q.Name,   // queue name
			key,      // routing key
			exchange, // exchange
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			conn.Close()
			return nil, ErrRabbitMQCreateExchangeMiddleware
		}
	}

	return &RabbitMQExchange{
		conn:      conn,
		exchange:  exchange,
		keys:      keys,
		queueName: q.Name,
		stopChan:  make(chan struct{}),
	}, nil
}

func (r *RabbitMQExchange) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	r.mu.Lock()
	if r.consuming {
		r.mu.Unlock()
		return nil
	}
	r.consuming = true
	r.tag = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	r.stopChan = make(chan struct{})
	r.mu.Unlock()
	ch := r.conn.GetChannel()

	msgs, err := ch.Consume(
		r.queueName, // queue
		r.tag,       // consumer tag
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
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
		case delivery, ok := <-msgs:
			if !ok {
				if r.conn.IsClosed() {
					return m.ErrMessageMiddlewareDisconnected
				}
				return m.ErrMessageMiddlewareMessage
			}

			ack := func() {
				_ = delivery.Ack(false)
			}
			nack := func() {
				_ = delivery.Nack(false, true)
			}

			callbackFunc(m.Message{Body: string(delivery.Body)}, ack, nack)
		}
	}
}

func (r *RabbitMQExchange) StopConsuming() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.consuming {
		return nil
	}

	r.consuming = false
	close(r.stopChan)

	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	err := r.conn.StopConsuming(r.tag)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (r *RabbitMQExchange) Send(msg m.Message) error {
	if r.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch := r.conn.GetChannel()

	for _, key := range r.keys {
		err := ch.PublishWithContext(
			ctx,
			r.exchange, // exchange
			key,        // routing key
			false,      // mandatory
			false,      // immediate
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
	}

	return nil
}

func (r *RabbitMQExchange) Close() error {
	r.mu.Lock()
	if r.consuming {
		r.consuming = false
		close(r.stopChan)
	}
	r.mu.Unlock()

	err := r.conn.Close()

	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
