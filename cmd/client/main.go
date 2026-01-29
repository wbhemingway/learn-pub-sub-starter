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
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Could not make a channel with the connection: %v", err)
	}

	fmt.Println("Starting Peril client...")
	userName, err := gamelogic.ClientWelcome()
	gs := gamelogic.NewGameState(userName)

	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeJSON(conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gs.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.TransientQueue,
		handlerMove(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	for {
		cmds := gamelogic.GetInput()
		switch cmds[0] {
		case "spawn":
			err = gs.CommandSpawn(cmds)
			if err != nil {
				fmt.Printf("Bad spawn command given: %v\n", err)
			}
		case "move":
			am, err := gs.CommandMove(cmds)
			if err != nil {
				fmt.Printf("Bad move command given: %v\n", err)
			}
			err = pubsub.PublishJSON(ch,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+gs.GetUsername(),
				am,
			)
			if err != nil {
				fmt.Printf("Failed to publish move command: %v\n", err)
			} else {
				fmt.Println("Move was published successfully")
			}
		case "status":
			gs.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("That command is not understood.")
		}
	}
}
