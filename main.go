package main

/**
The following is the code for a distributed Kafka Message Queue
Following are the specifications of the queue:
	The queue is a publisher subscriber model.
	A publisher can publish a message to a Topic.
	A consumer can subscribe to the Topic for the messages.
	A publisher can publish the messages into multiple topics.
	Multiple consumers can subscirbe to the same topic.
	Subscribed consumers should get notified when the message gets published to a Topic.
*/

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Consumer struct {
	id       uuid.UUID
	messages chan string
}

func NewConsumer() *Consumer {
	return &Consumer{
		id:       uuid.New(),
		messages: make(chan string, 10),
	}
}

type Publisher struct {
	id uuid.UUID
}

func NewPublisher() *Publisher {
	return &Publisher{
		id: uuid.New(),
	}
}

type Topic struct {
	id          uuid.UUID
	messages    []string
	subscribers []*Consumer
	mu          sync.Mutex
}

func NewTopic() *Topic {
	newUUID := uuid.New()
	messages := []string{}
	subscribers := []*Consumer{}
	var mu sync.Mutex

	return &Topic{
		newUUID,
		messages,
		subscribers,
		mu,
	}
}

func (c *Consumer) Subscribe(topic *Topic) {
	log.Printf("Consumer %s subscribed to the topic id: %s", c.id, topic.id)

	topic.subscribers = append(topic.subscribers, c)
}

func (p *Publisher) Publish(message string, topic *Topic) {
	log.Printf("Message %s gets published to topic id: %s by Pulisher: %s", message, topic.id, p.id)

	topic.mu.Lock()
	topic.messages = append(topic.messages, message)

	// defer is not used for the unlock, since the following code of notifying the subscribers can happen independently
	topic.mu.Unlock()

	for _, s := range topic.subscribers {
		if len(s.messages) == cap(s.messages) {
			log.Printf("Failed to publish the message into the channel!")
		}
		s.messages <- message
	}
}

func (c *Consumer) Listen() {
	for msg := range c.messages {
		log.Printf("Consumer id: %s has received the message: %s", c.id, msg)
	}
}

func main() {
	fmt.Println("Distributed Kafka Queue!")

	// Create Publishers and Consumers
	publisher1 := NewPublisher()
	publisher2 := NewPublisher()

	consumer1 := NewConsumer()
	consumer2 := NewConsumer()
	consumer3 := NewConsumer()
	consumer4 := NewConsumer()
	consumer5 := NewConsumer()

	topic1 := NewTopic()
	topic2 := NewTopic()

	consumer1.Subscribe(topic1)
	consumer2.Subscribe(topic1)
	consumer3.Subscribe(topic1)

	consumer4.Subscribe(topic1)
	consumer5.Subscribe(topic1)

	go consumer1.Listen()
	go consumer2.Listen()
	go consumer3.Listen()
	go consumer4.Listen()
	go consumer5.Listen()

	var wg sync.WaitGroup
	wg.Add(4)

	// Producers publishing messages
	go func() {
		defer wg.Done()
		publisher1.Publish(fmt.Sprintf("Publiser: %s sent a message!", publisher1.id), topic1)
	}()
	go func() {
		defer wg.Done()
		publisher2.Publish(fmt.Sprintf("Publiser: %s sent a message!", publisher2.id), topic1)
	}()
	go func() {
		defer wg.Done()
		publisher1.Publish(fmt.Sprintf("Publiser: %s sent a message!", publisher1.id), topic2)
	}()
	go func() {
		defer wg.Done()
		publisher2.Publish(fmt.Sprintf("Publiser: %s sent a message!", publisher2.id), topic2)
	}()

	wg.Wait()

	// Allow consumers to process remaining messages
	time.Sleep(1 * time.Second)

	close(consumer1.messages)
	close(consumer2.messages)
}
