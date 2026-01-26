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
	fmt.Println("Starting Peril client...")
	userName, err := gamelogic.ClientWelcome()
	ch, queue, err := pubsub.DeclareAndBind(rmqConn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, userName),
		routing.PauseKey,
		pubsub.TransientQueue)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %s declared and bound!\n", queue.Name)
	defer ch.Close()

	gameState := gamelogic.NewGameState(userName)
	for {
		cmds := gamelogic.GetInput()
		switch cmds[0] {
		case "spawn":
			err = gameState.CommandSpawn(cmds)
			if err != nil {
				fmt.Printf("Bad spawn command given: %v\n", err)
			}
		case "move":
			_, err := gameState.CommandMove(cmds)
			if err != nil {
				fmt.Printf("Bad move command given: %v\n", err)
			}
		case "status":
			gameState.CommandStatus()
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
