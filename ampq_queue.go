package amqp_queue

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

type Queue interface {
	Connect() error
	NotifyState(receiver chan string)
	GetState() string
	PublishOnQueue(message []byte, queueName string) error
	PublishOnExchange(message []byte, exchange *ExchangeDeclare, routingKey string) error
	SubscribeToQueue(queueName string, consumerName string, handleFunc func(delivery amqp.Delivery)) error
}

type RabbitQueue struct {
	url string
	channel *amqp.Channel
	connection *amqp.Connection
	state string
	stateReceivers []chan string
}


func NewMessageClient(connUrl string) (*RabbitQueue, error) {
	q := &RabbitQueue{
		url: connUrl,
	}
	return q, nil
}

func (rabbitQueue *RabbitQueue) Connect() (error) {
	con, ch, err := connectInternal(rabbitQueue.url, 2)
	if err != nil {
		return err
	}
	rabbitQueue.connection = con
	rabbitQueue.channel = ch
	rabbitQueue.state = "ready"
	go rabbitQueue.startReconnector()
	return nil
}

func (rabbitQueue *RabbitQueue) NotifyState (receiver chan string) {
	rabbitQueue.stateReceivers = append(rabbitQueue.stateReceivers, receiver)
}

func (rabbitQueue *RabbitQueue) GetState() string  {
	return rabbitQueue.state
}

func (rabbitQueue *RabbitQueue) PublishOnQueue(message []byte, queueName string) error {
	queue, err := rabbitQueue.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return err
	}

	err = rabbitQueue.channel.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        message,
		})

	return err
}

func (rabbitQueue *RabbitQueue) PublishOnExchange(message []byte, exchange *ExchangeDeclare, routingKey string) error {
	err := rabbitQueue.channel.ExchangeDeclare(
		exchange.Name,       // name
		exchange.Kind,       // type
		exchange.Durable,    // durable
		exchange.AutoDelete, // auto-deleted
		exchange.Internal,   // internal
		exchange.noWait,     // no-wait
		nil,                 // arguments
	)

	if err != nil {
		return err
	}

	err = rabbitQueue.channel.Publish(
		exchange.Name, // exchange
		routingKey,    // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		})

	return err
}

func (rabbitQueue *RabbitQueue) SubscribeToQueue(queueName string, consumerName string, handleFunc func(delivery amqp.Delivery)) error {
	queue, err := rabbitQueue.channel.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return err
	}

	msgs, err := rabbitQueue.channel.Consume(
		queue.Name,   // queue
		consumerName, // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)

	if err != nil {
		return err
	}

	go func(handleFunc func(delivery amqp.Delivery)) {
		for d := range msgs {
			handleFunc(d)
		}
	}(handleFunc)

	return nil
}

// re-establish the connection to RabbitMQ in case
// the connection has died
//
func (rabbitQueue *RabbitQueue) startReconnector() {
	for {
		rabbitCloseError := make(chan *amqp.Error)
		rabbitQueue.connection.NotifyClose(rabbitCloseError)
		rabbitErr := <-rabbitCloseError
		if rabbitErr != nil {
			rabbitQueue.state = "closed"
			for _, rcv := range rabbitQueue.stateReceivers {
				rcv <- rabbitQueue.state
			}
			for {
				log.Print("Reconnecting to RabbitMQ...")
				con, ch, err := connectInternal(rabbitQueue.url, 2)
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				rabbitQueue.connection = con
				rabbitQueue.channel = ch
				rabbitQueue.state = "open"
				for _, rcv := range rabbitQueue.stateReceivers {
					rcv <- rabbitQueue.state
				}
				log.Print("New RabbitMQ connection established")
				break
			}
		}
	}
}

func dummyReturn () string {
	return "Hi"
}

func connectInternal(url string, heartbeat int) (*amqp.Connection, *amqp.Channel, error) {
	duration := 5 * time.Second
	con, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: duration,
	})
	if err != nil {
		log.Print("Failed to open a connection", err)
		return nil, nil, err
	}
	ch, err := con.Channel()
	if err != nil {
		log.Print("Failed to open a channel", err)
		return nil, nil, err
	}
	return con, ch, nil
}
