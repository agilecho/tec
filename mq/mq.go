package mq

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/agilecho/tec/mq/amqp"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Message struct {
	delivery amqp.Delivery
	Id string
	Body string
}

type ReserveMessageFunc func(queue *Queue, message *Message)

type Config struct {
	Host string
	Port string
	User string
	Passwd string
	VHost string
	Exchange string
	Logs string
	Debug bool
}

func (this *Config) Set(key string, value string) {
	switch strings.ToLower(key) {
	case "host":
		this.Host = value
	case "port":
		this.Port = value
	case "user":
		this.User = value
	case "passwd":
		this.Passwd = value
	case "vhost":
		this.VHost = value
	case "exchange":
		this.Exchange = value
	case "logs":
		this.Logs = value
	case "debug":
		this.Debug, _ = strconv.ParseBool(value)
	}
}

type Queue struct {
	rabbitmq *RabbitMQ
	queue amqp.Queue
	name string
}

func (this *Queue) uuid() string {
	bytes := make([]byte, 48)
	io.ReadFull(rand.Reader, bytes)

	instance := md5.New()
	instance.Write([]byte(base64.URLEncoding.EncodeToString(bytes)))

	return hex.EncodeToString(instance.Sum(nil))
}

func (this *Queue) Put(data string) interface{} {
	if this.rabbitmq.channel == nil || this.queue.Name == "" {
		return false
	}

	err := this.rabbitmq.channel.Publish(this.rabbitmq.config.Exchange, this.name, false, false, amqp.Publishing {
		ContentType: "text/plain",
		MessageId: this.uuid(),
		Body: []byte(data),
	})

	if err != nil {
		this.rabbitmq.logger("mq.Put error:" + err.Error())
		return false
	}

	return true
}

func (this *Queue) Reserve(fun ReserveMessageFunc) {
	if this.rabbitmq.channel == nil || this.queue.Name == "" {
		return
	}

	deliveries, err := this.rabbitmq.channel.Consume(this.queue.Name, "", false, false, false, true, nil)
	if err != nil {
		this.rabbitmq.logger("mq.Reserve error:" + err.Error())
		return
	}

	for delivery := range deliveries {
		fun(this, &Message{
			delivery: delivery,
			Id: delivery.MessageId,
			Body: string(delivery.Body),
		})
	}
}

func (this *Queue) Delete(message *Message) bool {
	err := message.delivery.Ack(true)
	if err != nil {
		this.rabbitmq.logger("mq.Delete error:" + err.Error())
		return false
	}

	return true
}

type RabbitMQ struct {
	config *Config
	handle *amqp.Connection
	channel *amqp.Channel
	queues []*Queue
	mu sync.RWMutex
}

func (this *RabbitMQ) microtime() string {
	return strconv.FormatFloat(float64(time.Now().UnixNano() / 1e6) * 0.001, 'f', 4, 64)
}

func (this *RabbitMQ) logger(message string) {
	if this.config == nil {
		return
	}

	now := time.Now()

	var text = strings.Builder{}
	text.WriteString(fmt.Sprintf("%v%v%v%v%v%v", now.Format("2006-01-02 15:04:05"), "(", this.microtime(), ") ", message, "\r\n"))

	if this.config.Debug  {
		fmt.Print(text.String())
	}

	go func(data string, that *RabbitMQ) {
		that.mu.Lock()
		defer that.mu.Unlock()

		err := os.MkdirAll(that.config.Logs + "/" + time.Now().Format("200601"), os.ModePerm)
		if err != nil {
			return
		}

		file, _ := os.OpenFile(that.config.Logs + "/" + time.Now().Format("200601") + "/" + time.Now().Format("2006010215") + ".txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		file.WriteString(data)
	}(text.String(), this)
}

func (this *RabbitMQ) Connect() {
	var err error
	this.handle, err = amqp.Dial("amqp://" + this.config.User + ":" + this.config.Passwd + "@" + this.config.Host + ":" + this.config.Port + "/")
	if err != nil {
		this.logger("mq.Connect error:" + err.Error())
	}

	this.channel, err = this.handle.Channel()
	if err != nil {
		this.logger("mq.DirectQueue.Channel error:" + err.Error())
	}
}

func (this *RabbitMQ) Close() {
	if this.handle == nil {
		return
	}

	if !this.handle.IsClosed() {
		this.handle.Close()

		if this.channel == nil {
			return
		}

		this.channel.Close()
	}
}

func (this *RabbitMQ) DirectQueue(name string) *Queue {
	queue := &Queue{
		rabbitmq: this,
		name: name,
	}

	if this.handle == nil || this.channel == nil {
		return queue
	}

	err := this.channel.ExchangeDeclare(this.config.Exchange, amqp.ExchangeDirect, true, false, false, false, nil)
	if err != nil {
		this.logger("mq.DirectQueue.ExchangeDeclare error:" + err.Error())
	}

	queue.queue, err = this.channel.QueueDeclare(this.config.Exchange + "." + name, true, false, false, false, nil)
	if err != nil {
		this.logger("mq.DirectQueue.QueueDeclare error:" + err.Error())
		return queue
	}

	err = this.channel.QueueBind(this.config.Exchange + "." + name, name, this.config.Exchange, true, nil)
	if err != nil {
		this.logger("mq.DirectQueue.QueueBind error:" + err.Error())
		return queue
	}

	return queue
}

func (this *RabbitMQ) FanoutQueue(name string) *Queue {
	queue := &Queue{
		rabbitmq: this,
		name: name,
	}

	if this.handle == nil || this.channel == nil {
		return queue
	}

	err := this.channel.ExchangeDeclare(this.config.Exchange, amqp.ExchangeFanout, true, false, false, false, nil)
	if err != nil {
		this.logger("mq.FanoutQueue.ExchangeDeclare error:" + err.Error())
	}

	queue.queue, err = this.channel.QueueDeclare(this.config.Exchange + "." + name, true, false, false, false, nil)
	if err != nil {
		this.logger("mq.FanoutQueue.QueueDeclare error:" + err.Error())
		return queue
	}

	err = this.channel.QueueBind(this.config.Exchange + "." + name, name, this.config.Exchange, true, nil)
	if err != nil {
		this.logger("mq.FanoutQueue.QueueBind error:" + err.Error())
		return queue
	}

	return queue
}

func New(config *Config) *RabbitMQ {
	return &RabbitMQ{
		config:config,
	}
}

var handler *RabbitMQ

func Init(config *Config) {
	handler = New(config)
	handler.Connect()
}

func DirectQueue(name string) *Queue {
	return handler.DirectQueue(name)
}

func FanoutQueue(name string) *Queue {
	return handler.FanoutQueue(name)
}

func Close() {
	handler.Close()
}
