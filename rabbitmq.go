package rabbitmq

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

var mqConn *amqp.Connection
var mqChan *amqp.Channel

type Producer interface {
	MsgContent() string
}

type RetryProducer interface {
	MsgContent() string
}

type Receiver interface {
	Consumer([]byte) error
	FailAction([]byte) error
}

type RabbitMQ struct {
	connection        *amqp.Connection
	Channel           *amqp.Channel
	dns               string
	QueueName         string
	RoutingKey        string
	ExchangeName      string
	ExchangeType      string
	producerList      []Producer
	retryProducerList []Producer
	receiverList      []Receiver
}

type QueueExchange struct {
	QueueName    string
	RoutingKey   string
	ExchangeName string
	ExchangeType string
	Dns          string
}

// connect rabbitMQ
func (r *RabbitMQ) MqConnect() (err error) {
	mqConn, err = amqp.Dial(r.dns)
	r.connection = mqConn
	if err != nil {
		fmt.Printf("Failed to open MQ :%s \n", err)
	}
	return err
}

// Close RabbitMQ
func (r *RabbitMQ) CloseMqConnect() (err error) {
	err = r.connection.Close()
	if err != nil {
		fmt.Printf("Failed to close connection: %s \n", err)
	}
	return err
}

func (r *RabbitMQ) MqOpenChannel() (err error) {
	mqConn := r.connection
	r.Channel, err = mqConn.Channel()
	if err != nil {
		fmt.Printf("Failed to open MQ channel: %s \n", err)
	}
	return err
}

func (r *RabbitMQ) CloseMqChannel() (err error) {
	r.Channel.Close
	if err != nil {
		fmt.Printf("Failed to close MQ channel: %s \n", err)
	}

	return err
}

func NewMq(q QueueExchange) RabbitMQ {
	mq := RabbitMQ{
		QueueName:    q.QueueName,
		RoutingKey:   q.RoutingKey,
		ExchangeName: q.ExchangeName,
		ExchangeType: q.ExchangeType,
		dns:          q.Dns,
	}
	return mq
}

func (mq *RabbitMQ) sendMsg(body string) {
	err := mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil {
		log.Printf("Channel err : %s \n", err)
	}

	defer mq.CloseMqChannel()
	if mq.ExchangeName != "" {
		if mq.ExchangeType == "" {
			mq.ExchangeName = "direct"
		}
		err = ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err :s% \n", err)
		}
	}

	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, nil)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}

	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}

	if mq.ExchangeName != "" && mq.RoutingKey != "" {
		err = mq.Channel.Publish(
			mq.ExchangeName,
			mq.RoutingKey,
			false, // mandatory
			false, // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
	} else {
		err = mq.Channel.Publish(
			"", //exchange
			mq.QueueName,
			false, //mandatory
			false, //immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})
	}

}
