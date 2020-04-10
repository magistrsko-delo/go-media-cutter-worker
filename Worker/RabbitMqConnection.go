package Worker

import (
	"github.com/streadway/amqp"
	"log"
	"go-media-cutter-worker/Models"
)

type RabbitMqConnection struct {
	Conn *amqp.Connection
	Ch   *amqp.Channel
	q    amqp.Queue
	msgs <-chan amqp.Delivery
}

func initRabbitMqConnection(env *Models.Env) *RabbitMqConnection  {
	conn, err := amqp.Dial("amqp://" + env.RabbitUser + ":" + env.RabbitPassword + "@" + env.RabbitHost + ":5672")  // amqp://uros:uros123@localhost:5672/
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err := ch.QueueDeclare(
		env.RabbitQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,
		0,
		false,
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	return &RabbitMqConnection{
		Conn: conn,
		Ch:   ch,
		q:    q,
		msgs: msgs,
	}
}


func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}