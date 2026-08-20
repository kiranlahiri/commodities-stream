package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

func main() {
	apiKey := os.Getenv("MASSIVE_API_KEY")
	if apiKey == "" {
		log.Fatal("MASSIVE_API_KEY is not set")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is not set")
	}

	url := "wss://delayed.massive.com/futures"

	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatal("WebSocket connection failed:", err)
	}
	defer conn.Close()

	fmt.Println("Connected to Massive futures WebSocket")

	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "wti-aggregates-1s",
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	authMsg := map[string]string{
		"action": "auth",
		"params": apiKey,
	}

	err = conn.WriteJSON(authMsg)
	if err != nil {
		log.Fatal("Authentication message failed:", err)
	}

	subscribeMsg := map[string]string{
		"action": "subscribe",
		"params": "A.CLV6",
	}

	err = conn.WriteJSON(subscribeMsg)
	if err != nil {
		log.Fatal("Subscription failed:", err)
	}

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			log.Fatal("Failed to read message:", err)
		}

		fmt.Println("Response:", string(message))

		err = writer.WriteMessages(
			context.Background(),
			kafka.Message{
				Value: message,
			},
		)
		if err != nil {
			log.Println("Failed to write to Kafka:", err)
		}
	}
}
