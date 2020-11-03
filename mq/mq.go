package mq

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/agilecho/tec/mq/amqp"
	"github.com/agilecho/tec/mq/beanstalk"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Config struct {
	Type string
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
	case "type":
		this.Type = value
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

type Message struct {
	delivery amqp.Delivery
	Id string
	Body string
}

type ReserveMessageFunc func(handle MQ, msg *Message)

type MQ interface {
	Connect()
	Close()
	UseTube(tube string)
	FanoutTube(tube string)
	Put(data string, level uint32, delay time.Duration) interface{}
	Watch(tube string)
	Reserve(fun ReserveMessageFunc)
	Delete(message *Message) bool
}

var rwMu = &sync.RWMutex{}

type logger struct {
	config *Config
}

func (this *logger) log(message string) {
	if this.config == nil {
		return
	}

	now := time.Now()

	var text = strings.Builder{}

	text.WriteString(now.Format("2006-01-02 15:01:01"))
	text.WriteString("(")
	text.WriteString(strconv.FormatFloat(float64(now.UnixNano() / 1e6) * 0.0001, 'f', 4, 64))
	text.WriteString(") ")
	text.WriteString(message)
	text.WriteString("\r\n")

	if this.config.Debug  {
		fmt.Print(text.String())
	}

	go func(data string, config *Config) {
		rwMu.Lock()
		defer rwMu.Unlock()

		err := os.MkdirAll(config.Logs + "/" + time.Now().Format("20060102"), os.ModePerm)
		if err != nil {
			return
		}

		file, _ := os.OpenFile(config.Logs + "/" + time.Now().Format("20060102") + "/" + strconv.Itoa(time.Now().Hour()) + ".txt", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		defer file.Close()

		_, err = file.WriteString(data)
	}(text.String(), this.config)
}

type rabbitMQ struct {
	logger
	handle *amqp.Connection
	channel *amqp.Channel
	queue amqp.Queue
	exchange string
	tube string
}

func (this *rabbitMQ) Connect() {
	var err error
	this.handle, err = amqp.Dial("amqp://" + this.config.User + ":" + this.config.Passwd + "@" + this.config.Host + ":" + this.config.Port + "/")
	if err != nil {
		this.log("mq.rabbitMQ.Connect error:" + err.Error())
	}
}

func (this *rabbitMQ) Close() {
	if this.handle == nil {
		return
	}

	if !this.handle.IsClosed() {
		this.handle.Close()
		this.channel.Close()
	}
}

func (this *rabbitMQ) UseTube(tube string) {
	if this.handle == nil {
		return
	}

	this.tube = tube
	this.exchange = this.config.Exchange

	var err error
	this.channel, err = this.handle.Channel()
	if err != nil {
		this.log("mq.rabbitMQ.UseTube.Channel error:" + err.Error())
		return
	}

	err = this.channel.ExchangeDeclare(this.exchange, amqp.ExchangeDirect, true, false, false, false, nil)
	if err != nil {
		this.log("mq.rabbitMQ.UseTube.ExchangeDeclare error:" + err.Error())
	}
}

func (this *rabbitMQ) FanoutTube(tube string) {
	if this.handle == nil {
		return
	}

	this.tube = tube
	this.exchange = this.config.Exchange + ".fanout"

	var err error
	this.channel, err = this.handle.Channel()
	if err != nil {
		this.log("mq.rabbitMQ.FanoutTube.Channel error:" + err.Error())
		return
	}

	err = this.channel.ExchangeDeclare(this.exchange, amqp.ExchangeFanout, true, false, false, false, nil)
	if err != nil {
		this.log("mq.rabbitMQ.FanoutTube.ExchangeDeclare error:" + err.Error())
	}
}

func (this *rabbitMQ) Watch(tube string) {
	if this.channel == nil {
		return
	}

	var err error
	this.queue, err = this.channel.QueueDeclare(this.exchange + "." + this.tube, true, false, false, false, nil)
	if err != nil {
		this.log("mq.rabbitMQ.Watch.QueueDeclare error:" + err.Error())
		return
	}

	err = this.channel.QueueBind(this.exchange + "." + this.tube, this.tube, this.exchange, true, nil)
	if err != nil {
		this.log("mq.rabbitMQ.Watch.QueueBind error:" + err.Error())
		return
	}
}

func (this *rabbitMQ) Put(data string, level uint32, delay time.Duration) interface{} {
	if this.channel == nil {
		return false
	}

	err := this.channel.Publish(this.exchange, this.tube, false, false, amqp.Publishing {
		ContentType: "text/plain",
		MessageId: this.UUID(),
		Body: []byte(data),
	})

	if err != nil {
		this.log("mq.rabbitMQ.Put error:" + err.Error())
		return false
	}

	return true
}

func (this *rabbitMQ) UUID() string {
	bytes := make([]byte, 48)
	io.ReadFull(rand.Reader, bytes)

	instance := md5.New()
	instance.Write([]byte(base64.URLEncoding.EncodeToString(bytes)))

	return hex.EncodeToString(instance.Sum(nil))
}

func (this *rabbitMQ) Reserve(fun ReserveMessageFunc) {
	if this.channel == nil {
		return
	}

	deliveries, err := this.channel.Consume(this.queue.Name, "", false, false, false, true, nil)
	if err != nil {
		this.log("mq.rabbitMQ.Reserve error:" + err.Error())
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

func (this *rabbitMQ) Delete(message *Message) bool {
	err := message.delivery.Ack(true)
	if err != nil {
		this.log("mq.rabbitMQ.Delete error:" + err.Error())
		return false
	}

	return true
}

type beanstalkd struct {
	logger
	handle *beanstalk.Conn
}

func (this *beanstalkd) Connect() {
	var err error
	this.handle, err = beanstalk.Dial("tcp", this.config.Host + ":" + this.config.Port)
	if err != nil {
		this.log("mq.beanstalkd.Connect error:" + err.Error())
	}
}

func (this *beanstalkd) Close() {
	if this.handle != nil {
		this.handle.Close()
	}
}

func (this *beanstalkd) UseTube(tube string) {
	if this.handle == nil {
		return
	}

	this.handle.Tube.Name = tube
	this.handle.TubeSet.Name[tube] = true
}

func (this *beanstalkd) FanoutTube(tube string) {
	this.UseTube(tube)
}

func (this *beanstalkd) Watch(tube string) {

}

func (this *beanstalkd) Put(data string, level uint32, delay time.Duration) interface{} {
	if this.handle == nil {
		return 0
	}

	id, err := this.handle.Put([]byte(data), level, delay, 60 * time.Second)
	if err != nil {
		this.log("mq.beanstalkd.Put error:" + err.Error())
		return 0
	}

	return id
}

func (this *beanstalkd) Reserve(fun ReserveMessageFunc) {
	if this.handle == nil {
		return
	}

	message := Message{}
	id, body, err := this.handle.Reserve(1 * time.Second)
	if err != nil {
		this.log("mq.beanstalkd.Reserve error:" + err.Error())
		return
	}

	message.Id = strconv.FormatUint(id, 10)
	message.Body = string(body)

	fun(this, &message)
}

func (this *beanstalkd) Delete(message *Message) bool {
	if this.handle == nil {
		return false
	}

	id, err := strconv.ParseUint(message.Id, 10, 64)
	if err != nil {
		this.log("mq.beanstalkd.Delete error:" + err.Error())
		return false
	}

	err = this.handle.Delete(id)
	if err != nil {
		this.log("mq.beanstalkd.Delete error:" + err.Error())
		return false
	}

	return true
}

func New(config *Config) MQ {
	if config.Type == "rabbit" {
		return &rabbitMQ{logger:logger{config:config}}
	} else {
		return &beanstalkd{logger:logger{config:config}}
	}
}