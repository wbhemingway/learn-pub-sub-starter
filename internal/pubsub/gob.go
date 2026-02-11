package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/gob", Body: b.Bytes()})
	if err != nil {
		return err
	}
	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}
	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QoS: %v", err)
	}
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	unmarshaller := func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		decoder := gob.NewDecoder(buffer)
		var target T
		err := decoder.Decode(&target)
		return target, err
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message : %v\n", err)
			}
			ack := handler(target)
			switch ack {
			case Ack:
				msg.Ack(false)
				log.Print("message acknowledged")
			case NackDiscard:
				msg.Nack(false, false)
				log.Print("message discarded")
			case NackRequeue:
				msg.Nack(false, true)
				log.Print("message requeued")
			}
		}
	}()
	return nil
}
