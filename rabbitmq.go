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

func (mq *RabbitMQ) sendRetryMsg(body string, retry_nums int32, arg ...string) {
	err := mq.MqOpenChannel()
	ch := mq.Channel

	if err != nil {
		log.Printf("Channel err :%s \n", err)
	}
	defer mq.CloseMqChannel()

	if mq.ExchangeName != "" {
		if mq.ExchangeName == "" {
			mq.ExchangeName = "direct"
		}
		err = ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
		}
	}

	oldRoutingKey := arg[0]
	oldExchangeName := arg[1]

	table := make(map[string]interface{}, 3)
	table["x-dead-letter-routing-key"] = oldRoutingKey
	if oldExchangeName != "" {
		table["x-dead-letter-exchange"] = oldExchangeName
	} else {
		mq.ExchangeName = ""
		table["x-dead-letter-exchange"] = ""
	}

	table["x-message-ttl"] = int64(20000)

	_, err = ch.QueueDeclare(mq.QueueName, true, false, false, false, table)
	if err != nil {
		log.Printf("QueueDeclare err :%s \n", err)
	}

	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		err = ch.QueueBind(mq.QueueName, mq.RoutingKey, mq.ExchangeName, false, nil)
		if err != nil {
			log.Printf("QueueBind err :%s \n", err)
		}
	}

	header := make(map[string]interface{}, 1)
	header["retry_nums"] = retry_nums + int32(1)

	var ttl_exchange string
	var ttl_routkey string

	if mq.ExchangeName != "" {
		ttl_exchange = mq.ExchangeName
	} else {
		ttl_exchange = ""
	}

	if mq.RoutingKey != "" && mq.ExchangeName != "" {
		ttl_routkey = mq.RoutingKey
	} else {
		ttl_routkey = mq.QueueName
	}

	//fmt.Printf("ttl_exchange:%s,ttl_routkey:%s \n",ttl_exchange,ttl_routkey)
	err = mq.Channel.Publish(
		ttl_exchange, // exchange
		ttl_routkey,  // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
			Headers:     header,
		})

	if err != nil {
		fmt.Printf("MQ任务发送失败:%s \n", err)

	}

}

func (mq *RabbitMQ) ListenReceiver(receiver Receiver, routineNum int) {
	err := mq.MqOpenChannel()
	ch := mq.Channel
	if err != nil {
		log.Printf("Channel err  :%s \n", err)
	}

	defer mq.CloseMqChannel()
	if mq.ExchangeName != "" {
		if mq.ExchangeType == "" {
			mq.ExchangeType = "direct"
		}
		err = ch.ExchangeDeclare(mq.ExchangeName, mq.ExchangeType, true, false, false, false, nil)
		if err != nil {
			log.Printf("ExchangeDeclare err  :%s \n", err)
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

	err = ch.Qos(1, 0, false)
	mqList, err := ch.Consume(mq.QueueName, "", false, false, false, false, nil)
	if err != nil {
		log.Printf("Consume err :%s \n", err)
	}

	for msg := range mqList {
		retry_nums, ok := msg.Headers["retry_nums"].(int32)
		if !ok {
			retry_nums = int32(0)
		}

		err := receiver.Consumer(msg.Body)
		if err != nil {
			if retry_nums < 3 {
				fmt.Println(string(msg.Body))
				fmt.Printf("Failse to comsume msg, ttl queue \n")
				//retry
			} else {
				fmt.Printf("Failed to cosume 3 times \n")
				receiver.FailAction(msg.Body)
			}
			err = msg.Ack(true)
			if err != nil {
				fmt.Printf("Failed to Ackonwledge:%s \n", err)
			}
		} else {
			err = msg.Ack(true)

			if err != nil {
				fmt.Printf("Failed to Ackonwledge:%s \n", err)
			}
		}
	}

}
