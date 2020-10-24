package amqp

import (
	"errors"
	"time"
)

var errDeliveryNotInitialized = errors.New("delivery not initialized")

type Acknowledger interface {
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple bool, requeue bool) error
	Reject(tag uint64, requeue bool) error
}

type Delivery struct {
	Acknowledger Acknowledger
	Headers Table
	ContentType     string
	ContentEncoding string
	DeliveryMode    uint8
	Priority        uint8
	CorrelationId   string
	ReplyTo         string
	Expiration      string
	MessageId       string
	Timestamp       time.Time
	Type            string
	UserId          string
	AppId           string
	ConsumerTag string
	MessageCount uint32

	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string

	Body []byte
}

func newDelivery(channel *Channel, msg messageWithContent) *Delivery {
	props, body := msg.getContent()

	delivery := Delivery{
		Acknowledger: channel,

		Headers:         props.Headers,
		ContentType:     props.ContentType,
		ContentEncoding: props.ContentEncoding,
		DeliveryMode:    props.DeliveryMode,
		Priority:        props.Priority,
		CorrelationId:   props.CorrelationId,
		ReplyTo:         props.ReplyTo,
		Expiration:      props.Expiration,
		MessageId:       props.MessageId,
		Timestamp:       props.Timestamp,
		Type:            props.Type,
		UserId:          props.UserId,
		AppId:           props.AppId,

		Body: body,
	}

	// Properties for the delivery types
	switch m := msg.(type) {
	case *basicDeliver:
		delivery.ConsumerTag = m.ConsumerTag
		delivery.DeliveryTag = m.DeliveryTag
		delivery.Redelivered = m.Redelivered
		delivery.Exchange = m.Exchange
		delivery.RoutingKey = m.RoutingKey

	case *basicGetOk:
		delivery.MessageCount = m.MessageCount
		delivery.DeliveryTag = m.DeliveryTag
		delivery.Redelivered = m.Redelivered
		delivery.Exchange = m.Exchange
		delivery.RoutingKey = m.RoutingKey
	}

	return &delivery
}

func (d Delivery) Ack(multiple bool) error {
	if d.Acknowledger == nil {
		return errDeliveryNotInitialized
	}
	return d.Acknowledger.Ack(d.DeliveryTag, multiple)
}

func (d Delivery) Reject(requeue bool) error {
	if d.Acknowledger == nil {
		return errDeliveryNotInitialized
	}
	return d.Acknowledger.Reject(d.DeliveryTag, requeue)
}

func (d Delivery) Nack(multiple, requeue bool) error {
	if d.Acknowledger == nil {
		return errDeliveryNotInitialized
	}
	return d.Acknowledger.Nack(d.DeliveryTag, multiple, requeue)
}
