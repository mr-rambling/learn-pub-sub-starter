package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return fmt.Errorf("could not create channel: %v", err)
	}

	msgs, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("could not create channel: %v", err)
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			var val T
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				fmt.Println("Failed to unmarshal message:", err)
				continue
			}
			handler(val)
			msg.Ack(false)
		}
	}()

	return nil
}
