package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rmqAddr := "amqp://guest:guest@localhost:5672/"
	rmqConn, err := amqp.Dial(rmqAddr)
	if err != nil {
		log.Fatalf("Could not make RMQ connection to %s: %v", rmqAddr, err)
	}

	defer rmqConn.Close()
	fmt.Printf("Connection to RMQ adress %s was successful!\n", rmqAddr)

	amqpChann, err := rmqConn.Channel()
	if err != nil {
		log.Fatalf("Could not make RMQ cchannel: %v", err)
	}

	fmt.Println("Starting Peril server...")

	gamelogic.PrintServerHelp()

	for {
		cmds := gamelogic.GetInput()
		switch cmds[0] {
		case "pause":
			fmt.Println("Sending pause message")
			pubsub.PublishJSON(amqpChann,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume message")
			pubsub.PublishJSON(amqpChann,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Server exiting")
			return
		default:
			fmt.Println("That command is not understood.")
		}

	}

	fmt.Println("Peril server is shutting down...")
}
