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
	conn, err := amqp.Dial(rmqAddr)
	if err != nil {
		log.Fatalf("Could not make RMQ connection to %s: %v", rmqAddr, err)
	}

	defer conn.Close()
	fmt.Printf("Connection to RMQ adress %s was successful!\n", rmqAddr)

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Could not make RMQ cchannel: %v", err)
	}
	err = pubsub.SubscribeGob(
			conn,
			routing.ExchangePerilTopic,
			routing.GameLogSlug,
			routing.GameLogSlug+".*",
			pubsub.DurableQueue,
			handlerLogs(),
		)
		if err != nil {
			log.Fatalf("could not starting consuming logs: %v", err)
		}

	if err != nil {
		log.Fatalf("could not subscribe to peril topic: %v", err)
	}

	fmt.Println("Starting Peril server...")

	gamelogic.PrintServerHelp()

	for {
		cmds := gamelogic.GetInput()
		switch cmds[0] {
		case "pause":
			fmt.Println("Sending pause message")
			pubsub.PublishJSON(publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume message")
			pubsub.PublishJSON(publishCh,
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
}
