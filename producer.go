package main

import (
    "fmt"
    "log"

    "github.com/Shopify/sarama"
)

func main() {
    // Define the Kafka broker addresses
    brokers := []string{"localhost:9092"}

    // Set up the producer configuration
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Retry.Max = 5
    config.Producer.Return.Successes = true

    // Create a new producer
    producer, err := sarama.NewSyncProducer(brokers, config)
    if err != nil {
        log.Fatalln("Failed to create producer:", err)
    }
    defer func() {
        if err := producer.Close(); err != nil {
            log.Fatalln("Failed to close producer:", err)
        }
    }()

    // Create a new message to send
    message := &sarama.ProducerMessage{
        Topic: "my-topic",
        Value: sarama.StringEncoder("Hello, Kafka!"),
    }

    // Send the message
    partition, offset, err := producer.SendMessage(message)
    if err != nil {
        log.Fatalln("Failed to send message:", err)
    }

    fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
}
