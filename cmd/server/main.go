package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	const rabbitConnStr = "amqp://guest:guest@localhost:5672/"
	const queueKey = routing.GameLogSlug + ".*"

	conn, err := amqp.Dial(rabbitConnStr)
	defer conn.Close()
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	} else {
		fmt.Println("Connected to RabbitMQ successfully.")
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		routing.GameLogSlug,
		queueKey,
		pubsub.SimpleQueueDurable,
	)

	if err != nil {
		fmt.Println("Failed to declare and bind queue:", err)
		return
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	publishCh, err := conn.Channel()
	defer publishCh.Close()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}

	gamelogic.PrintServerHelp()

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}

		switch inputs[0] {
		case "pause":
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			fmt.Println("Game paused.")
		case "resume":
			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			fmt.Println("Game resumed.")
		case "quit":
			fmt.Println("Quitting server...")
			return
		default:
			fmt.Println("Unknown command.")
		}

		if err != nil {
			fmt.Println("Failed to publish message:", err)
			return
		}
	}

}
