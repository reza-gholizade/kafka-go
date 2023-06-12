package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/Shopify/sarama"
)

func main() {
    // Define the Kafka broker addresses
    brokers := []string{"localhost:9092"}

    // Set up the consumer configuration
    config := sarama.NewConfig()
    config.Consumer.Return.Errors = true

    // Create a new consumer group
    consumer, err := sarama.NewConsumerGroup(brokers, "my-group", config)
    if err != nil {
        log.Fatalln("Failed to create consumer group:", err)
    }
    defer func() {
        if err := consumer.Close(); err != nil {
            log.Fatalln("Failed to close consumer group:", err)
        }
    }()

    // Define the consumer group handler
    handler := &consumerGroupHandler{
        ready: make(chan bool),
    }

    // Start consuming messages
    ctx := signalContext()
    for {
        if err := consumer.Consume(ctx, []string{"my-topic"}, handler); err != nil {
            log.Fatalln("Failed to consume messages:", err)
        }
        if ctx.Err() != nil {
            return
        }
        handler.ready = make(chan bool)
    }
}

type consumerGroupHandler struct {
    ready chan bool
}

func (h *consumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
    close(h.ready)
    return nil
}

func (h *consumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
    return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for message := range claim.Messages() {
        fmt.Printf("Message claimed: topic=%s partition=%d offset=%d value=%s\n",
            message.Topic, message.Partition, message.Offset, string(message.Value))
        session.MarkMessage(message, "")
    }
    return nil
}

func signalContext() context.Context {
    ctx, cancel := context.WithCancel(context.Background())
    c := make(chan os.Signal, 1)
    signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        select {
        case <-c:
            cancel()
        case <-ctx.Done():
        }
    }()
    return ctx
}