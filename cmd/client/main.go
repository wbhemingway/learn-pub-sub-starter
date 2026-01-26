package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
