package amqp_queue

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"testing"
)

func TestPublishOnQueue(t *testing.T) {

	conn := setupRabbitMQ()

	client := NewMessageClient(conn)

	testData := "some test string"
	data, _ := json.Marshal(testData)

	err := client.PublishOnQueue(data, "testQueue")

	if err != nil {
		t.Fatal(err)
	}
}

func TestPublishOnTopic(t *testing.T) {

	conn := setupRabbitMQ()

	client := NewMessageClient(conn)

	testData := "some test string"
	data, _ := json.Marshal(testData)

	err := client.PublishOnExchange(data, &ExchangeDeclare{
		Name:       "test",
		Kind:       "topic",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		noWait:     false,
	}, "test.message")

	if err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeToQueue(t *testing.T) {

	conn := setupRabbitMQ()

	client := NewMessageClient(conn)

	err := client.SubscribeToQueue("testQueue", "consumerTest", func(delivery amqp.Delivery) {

	})

	if err != nil {
		t.Fatal(err)
	}
}

func setupRabbitMQ() *amqp.Connection {
	conn, err := amqp.Dial("amqp://guest:guest@52.59.231.30:5672/")

	if err != nil {
		log.Fatalln(err)
	}

	return conn
}
