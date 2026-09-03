package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

var (
	websocketMessagesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "collector_websocket_messages_total",
			Help: "Total number of raw WebSocket messages received from Massive.",
		},
	)
	massiveEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "collector_massive_events_total",
			Help: "Total number of Massive events received by event type.",
		},
		[]string{"event_type"},
	)
	kafkaWritesTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "collector_kafka_writes_total",
			Help: "Total number of successful Kafka writes.",
		},
	)
	kafkaWriteFailuresTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "collector_kafka_write_failures_total",
			Help: "Total number of failed Kafka writes.",
		},
	)
	websocketReconnectsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "collector_websocket_reconnects_total",
			Help: "Total number of WebSocket reconnect attempts.",
		},
	)
	lastMarketEventTimestamp = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "collector_last_market_event_timestamp_seconds",
			Help: "Unix timestamp in seconds of the last aggregate market event received.",
		},
	)
)

type massiveEvent struct {
	EventType string   `json:"ev"`
	StartMS   *float64 `json:"s,omitempty"`
	EndMS     *float64 `json:"e,omitempty"`
}

func main() {
	prometheus.MustRegister(
		websocketMessagesTotal,
		massiveEventsTotal,
		kafkaWritesTotal,
		kafkaWriteFailuresTotal,
		websocketReconnectsTotal,
		lastMarketEventTimestamp,
	)

	apiKey := os.Getenv("MASSIVE_API_KEY")
	if apiKey == "" {
		log.Fatal("MASSIVE_API_KEY is not set")
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		log.Fatal("KAFKA_BROKER is not set")
	}

	metricsAddr := os.Getenv("METRICS_ADDR")
	if metricsAddr == "" {
		metricsAddr = ":2112"
	}

	go serveMetrics(metricsAddr)

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

		websocketMessagesTotal.Inc()
		shouldPublish := recordMassiveEvents(message)

		fmt.Println("Response:", string(message))

		if !shouldPublish {
			continue
		}

		err = writer.WriteMessages(
			context.Background(),
			kafka.Message{
				Value: message,
			},
		)
		if err != nil {
			kafkaWriteFailuresTotal.Inc()
			log.Println("Failed to write to Kafka:", err)
			continue
		}

		kafkaWritesTotal.Inc()
	}
}

func serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	log.Printf("Serving Prometheus metrics on %s/metrics", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal("Metrics server failed:", err)
	}
}

func recordMassiveEvents(message []byte) bool {
	var events []massiveEvent
	if err := json.Unmarshal(message, &events); err != nil {
		massiveEventsTotal.WithLabelValues("unknown").Inc()
		return false
	}

	shouldPublish := false
	for _, event := range events {
		eventType := event.EventType
		if eventType == "" {
			eventType = "unknown"
		}

		massiveEventsTotal.WithLabelValues(eventType).Inc()

		if event.EventType == "A" {
			shouldPublish = true
			lastMarketEventTimestamp.Set(eventTimestamp(event))
		}
	}

	return shouldPublish
}

func eventTimestamp(event massiveEvent) float64 {
	if event.EndMS != nil {
		return *event.EndMS / float64(time.Second/time.Millisecond)
	}
	if event.StartMS != nil {
		return *event.StartMS / float64(time.Second/time.Millisecond)
	}

	return float64(time.Now().Unix())
}
