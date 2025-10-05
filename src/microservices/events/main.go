package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

// Event models
type MovieEvent struct {
	MovieID int    `json:"movie_id"`
	Title   string `json:"title"`
	Action  string `json:"action"`
	UserID  int    `json:"user_id"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID  int     `json:"payment_id"`
	UserID     int     `json:"user_id"`
	Amount     float64 `json:"amount"`
	Status     string  `json:"status"`
	Timestamp  string  `json:"timestamp"`
	MethodType string  `json:"method_type"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

var (
	producer sarama.SyncProducer
	consumer sarama.Consumer
	wg       sync.WaitGroup
)

func main() {
	// Initialize Kafka producer and consumer
	initKafka()
	defer producer.Close()
	defer consumer.Close()

	// Start Kafka consumer in background
	startConsumer()

	// Set up HTTP routes
	http.HandleFunc("/health", healthHandler)
	http.HandleFunc("/api/events/health", healthHandler)
	http.HandleFunc("/api/events/movie", handleMovieEvent)
	http.HandleFunc("/api/events/user", handleUserEvent)
	http.HandleFunc("/api/events/payment", handlePaymentEvent)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events microservice on port %s", port)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		cancel()
	}()

	// Start HTTP server
	server := &http.Server{Addr: ":" + port}
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server failed:", err)
		}
	}()

	// Wait for shutdown signal
	<-ctx.Done()

	// Shutdown server
	server.Shutdown(context.Background())

	// Wait for consumer to finish
	wg.Wait()
	log.Println("Service stopped")
}

func initKafka() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Consumer.Return.Errors = true

	// Retry logic for Kafka connection
	maxRetries := 30
	retryDelay := 2 * time.Second

	var err error
	for i := 0; i < maxRetries; i++ {
		producer, err = sarama.NewSyncProducer([]string{brokers}, config)
		if err == nil {
			break
		}
		log.Printf("Failed to start Kafka producer (attempt %d/%d): %v", i+1, maxRetries, err)
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	if err != nil {
		log.Fatal("Failed to start Kafka producer after all retries:", err)
	}

	for i := 0; i < maxRetries; i++ {
		consumer, err = sarama.NewConsumer([]string{brokers}, config)
		if err == nil {
			break
		}
		log.Printf("Failed to start Kafka consumer (attempt %d/%d): %v", i+1, maxRetries, err)
		if i < maxRetries-1 {
			time.Sleep(retryDelay)
		}
	}
	if err != nil {
		log.Fatal("Failed to start Kafka consumer after all retries:", err)
	}

	log.Println("Successfully connected to Kafka (producer and consumer)")
}

func waitForTopics(topics []string) {
	maxRetries := 30
	retryDelay := 2 * time.Second

	for _, topic := range topics {
		for i := 0; i < maxRetries; i++ {
			_, err := consumer.Partitions(topic)
			if err == nil {
				log.Printf("Topic %s is ready", topic)
				break
			}
			log.Printf("Waiting for topic %s to be created (attempt %d/%d): %v", topic, i+1, maxRetries, err)
			if i < maxRetries-1 {
				time.Sleep(retryDelay)
			} else {
				log.Printf("Warning: Topic %s not found after %d attempts, continuing anyway", topic, maxRetries)
			}
		}
	}
}

func startConsumer() {
	topics := []string{"movie-events", "user-events", "payment-events"}

	// Wait for topics to be created
	waitForTopics(topics)

	for _, topic := range topics {
		wg.Add(1)
		go consumeTopic(topic)
	}
}

func consumeTopic(topic string) {
	defer wg.Done()

	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("Failed to get partitions for topic %s: %v", topic, err)
		return
	}

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Printf("Failed to start consumer for topic %s partition %d: %v", topic, partition, err)
			continue
		}
		defer pc.AsyncClose()

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				processEvent(topic, message)
			}
		}(pc)
	}
}

func processEvent(topic string, message *sarama.ConsumerMessage) {
	log.Printf("Processing event from topic %s, partition %d, offset %d: %s",
		topic, message.Partition, message.Offset, string(message.Value))

	// Parse and log the event based on topic
	switch topic {
	case "movie-events":
		var event MovieEvent
		if err := json.Unmarshal(message.Value, &event); err == nil {
			log.Printf("MOVIE EVENT: User %d %s movie '%s' (ID: %d)",
				event.UserID, event.Action, event.Title, event.MovieID)
		}
	case "user-events":
		var event UserEvent
		if err := json.Unmarshal(message.Value, &event); err == nil {
			log.Printf("USER EVENT: User %d (%s) performed action: %s",
				event.UserID, event.Username, event.Action)
		}
	case "payment-events":
		var event PaymentEvent
		if err := json.Unmarshal(message.Value, &event); err == nil {
			log.Printf("PAYMENT EVENT: User %d made payment %d for $%.2f with status: %s",
				event.UserID, event.PaymentID, event.Amount, event.Status)
		}
	}
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"status": true})
}

func handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create event with timestamp
	timestamp := time.Now().Format(time.RFC3339)
	eventData := map[string]interface{}{
		"movie_id":  event.MovieID,
		"title":     event.Title,
		"action":    event.Action,
		"user_id":   event.UserID,
		"timestamp": timestamp,
	}

	eventJSON, _ := json.Marshal(eventData)

	// Create Event object for response
	eventObj := Event{
		ID:        fmt.Sprintf("movie-%d-%s", event.MovieID, event.Action),
		Type:      "movie",
		Timestamp: timestamp,
		Payload:   eventData,
	}

	// Publish to Kafka
	message := &sarama.ProducerMessage{
		Topic: "movie-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("Movie event published to partition %d at offset %d", partition, offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     eventObj,
	})
}

func handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create event data
	eventData := map[string]interface{}{
		"user_id":   event.UserID,
		"username":  event.Username,
		"action":    event.Action,
		"timestamp": event.Timestamp,
	}

	eventJSON, _ := json.Marshal(eventData)

	// Create Event object for response
	eventObj := Event{
		ID:        fmt.Sprintf("user-%d-%s", event.UserID, event.Action),
		Type:      "user",
		Timestamp: event.Timestamp,
		Payload:   eventData,
	}

	// Publish to Kafka
	message := &sarama.ProducerMessage{
		Topic: "user-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("User event published to partition %d at offset %d", partition, offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     eventObj,
	})
}

func handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var event PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Create event data
	eventData := map[string]interface{}{
		"payment_id":  event.PaymentID,
		"user_id":     event.UserID,
		"amount":      event.Amount,
		"status":      event.Status,
		"timestamp":   event.Timestamp,
		"method_type": event.MethodType,
	}

	eventJSON, _ := json.Marshal(eventData)

	// Create Event object for response
	eventObj := Event{
		ID:        fmt.Sprintf("payment-%d-%s", event.PaymentID, event.Status),
		Type:      "payment",
		Timestamp: event.Timestamp,
		Payload:   eventData,
	}

	// Publish to Kafka
	message := &sarama.ProducerMessage{
		Topic: "payment-events",
		Value: sarama.StringEncoder(eventJSON),
	}

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		http.Error(w, "Failed to publish event", http.StatusInternalServerError)
		return
	}

	log.Printf("Payment event published to partition %d at offset %d", partition, offset)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(EventResponse{
		Status:    "success",
		Partition: partition,
		Offset:    offset,
		Event:     eventObj,
	})
}
