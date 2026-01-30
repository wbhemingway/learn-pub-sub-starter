package pubsub

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	TransientQueue SimpleQueueType = "transient"
	DurableQueue   SimpleQueueType = "durable"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	chann, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := chann.QueueDeclare(queueName,
		queueType == DurableQueue,
		queueType == TransientQueue,
		queueType == TransientQueue,
		false,
		amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = chann.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return chann, queue, nil
}
