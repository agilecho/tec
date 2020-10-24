package amqp

import (
	"time"
)

type Return struct {
	ReplyCode  uint16
	ReplyText  string
	Exchange   string
	RoutingKey string
	ContentType     string
	ContentEncoding string
	Headers         Table
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

	Body []byte
}

func newReturn(msg basicReturn) *Return {
	props, body := msg.getContent()

	return &Return{
		ReplyCode:  msg.ReplyCode,
		ReplyText:  msg.ReplyText,
		Exchange:   msg.Exchange,
		RoutingKey: msg.RoutingKey,

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
}
