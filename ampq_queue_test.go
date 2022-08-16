package amqp_queue

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"testing"
)

func TestPublishOnQueue(t *testing.T) {

	client, err := NewMessageClient("amqp://guest:guest@192.168.178.62:5672/")
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}

	testData := "some test string"
	data, _ := json.Marshal(testData)

	err = client.PublishOnQueue(data, "testQueue")

	if err != nil {
		t.Fatal(err)
	}
}

func TestPublishOnTopic(t *testing.T) {

	client, err := NewMessageClient("amqp://guest:guest@192.168.178.62:5672/")
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}

	testData := "some test string"
	data, _ := json.Marshal(testData)

	err = client.PublishOnExchange("test", "test.message", &amqp.Publishing{
		ContentType: "text/plain",
		Body:        data,
	})

	if err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeToQueue(t *testing.T) {

	client, err := NewMessageClient("amqp://guest:guest@192.168.178.62:5672/")
	if err != nil {
		t.Fatal(err)
	}
	err = client.Connect()
	if err != nil {
		t.Fatal(err)
	}

	err = client.SubscribeToQueue("testQueue", "consumerTest", func(delivery amqp.Delivery) {

	})

	if err != nil {
		t.Fatal(err)
	}
}
