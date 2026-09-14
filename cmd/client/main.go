package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	const rabbitConnStr = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnStr)
	defer conn.Close()
	if err != nil {
		fmt.Println("Failed to connect to RabbitMQ:", err)
		return
	} else {
		fmt.Println("Connected to RabbitMQ successfully.")
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}

	gamestate := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gamestate.GetUsername(),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gamestate),
	)

	if err != nil {
		fmt.Println("Failed to subscribe to pause messages:", err)
		return
	}

	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerNewMsg(gamestate),
	)

	if err != nil {
		fmt.Println("Failed to subscribe to pause messages:", err)
		return
	}

	ch, err := conn.Channel()
	defer ch.Close()
	if err != nil {
		fmt.Println("Failed to open a channel:", err)
		return
	}

	for {
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			continue
		}

		switch inputs[0] {
		case "spawn":
			err = gamestate.CommandSpawn(inputs)
			if err != nil {
				fmt.Println("Error spawning unit:", err)
			}
		case "move":
			move, err := gamestate.CommandMove(inputs)
			if err != nil {
				fmt.Println("Error moving unit:", err)
			}
			fmt.Println("Move command sent.")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+username,
				move,
			)
			if err != nil {
				fmt.Println("Failed to publish move message:", err)
			}
			fmt.Println("Move published successfully.")
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command.")
		}
	}
}
