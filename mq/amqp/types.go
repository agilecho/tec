package amqp

import (
	"fmt"
	"io"
	"time"
)

const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

var (
	ErrClosed = &Error{Code: ChannelError, Reason: "channel/connection is not open"}
	ErrChannelMax = &Error{Code: ChannelError, Reason: "channel id space exhausted"}
	ErrSASL = &Error{Code: AccessRefused, Reason: "SASL could not negotiate a shared mechanism"}
	ErrCredentials = &Error{Code: AccessRefused, Reason: "username or password not allowed"}
	ErrVhost = &Error{Code: AccessRefused, Reason: "no access to this vhost"}
	ErrSyntax = &Error{Code: SyntaxError, Reason: "invalid field or value inside of a frame"}
	ErrFrame = &Error{Code: FrameError, Reason: "frame could not be parsed"}
	ErrCommandInvalid = &Error{Code: CommandInvalid, Reason: "unexpected command received"}
	ErrUnexpectedFrame = &Error{Code: UnexpectedFrame, Reason: "unexpected frame received"}
	ErrFieldType = &Error{Code: SyntaxError, Reason: "unsupported table field type"}
)

type Error struct {
	Code    int
	Reason  string
	Server  bool
	Recover bool
}

func newError(code uint16, text string) *Error {
	return &Error{
		Code:    int(code),
		Reason:  text,
		Recover: isSoftExceptionCode(int(code)),
		Server:  true,
	}
}

func (e Error) Error() string {
	return fmt.Sprintf("Exception (%d) Reason: %q", e.Code, e.Reason)
}

type properties struct {
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
	reserved1       string
}

const (
	Transient  uint8 = 1
	Persistent uint8 = 2
)

const (
	flagContentType     = 0x8000
	flagContentEncoding = 0x4000
	flagHeaders         = 0x2000
	flagDeliveryMode    = 0x1000
	flagPriority        = 0x0800
	flagCorrelationId   = 0x0400
	flagReplyTo         = 0x0200
	flagExpiration      = 0x0100
	flagMessageId       = 0x0080
	flagTimestamp       = 0x0040
	flagType            = 0x0020
	flagUserId          = 0x0010
	flagAppId           = 0x0008
	flagReserved1       = 0x0004
)

type Queue struct {
	Name      string
	Messages  int
	Consumers int
}

type Publishing struct {
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
	Body []byte
}

type Blocking struct {
	Active bool
	Reason string
}

type Confirmation struct {
	DeliveryTag uint64
	Ack         bool
}

type Decimal struct {
	Scale uint8
	Value int32
}

type Table map[string]interface{}

func validateField(f interface{}) error {
	switch fv := f.(type) {
	case nil, bool, byte, int, int16, int32, int64, float32, float64, string, []byte, Decimal, time.Time:
		return nil

	case []interface{}:
		for _, v := range fv {
			if err := validateField(v); err != nil {
				return fmt.Errorf("in array %s", err)
			}
		}
		return nil

	case Table:
		for k, v := range fv {
			if err := validateField(v); err != nil {
				return fmt.Errorf("table field %q %s", k, err)
			}
		}
		return nil
	}

	return fmt.Errorf("value %t not supported", f)
}

func (t Table) Validate() error {
	return validateField(t)
}

type tagSet []uint64

func (set tagSet) Len() int              { return len(set) }
func (set tagSet) Less(i, j int) bool    { return (set)[i] < (set)[j] }
func (set tagSet) Swap(i, j int)         { (set)[i], (set)[j] = (set)[j], (set)[i] }
func (set *tagSet) Push(tag interface{}) { *set = append(*set, tag.(uint64)) }
func (set *tagSet) Pop() interface{} {
	val := (*set)[len(*set)-1]
	*set = (*set)[:len(*set)-1]
	return val
}

type message interface {
	id() (uint16, uint16)
	wait() bool
	read(io.Reader) error
	write(io.Writer) error
}

type messageWithContent interface {
	message
	getContent() (properties, []byte)
	setContent(properties, []byte)
}

type frame interface {
	write(io.Writer) error
	channel() uint16
}

type reader struct {
	r io.Reader
}

type writer struct {
	w io.Writer
}

type protocolHeader struct{}

func (protocolHeader) write(w io.Writer) error {
	_, err := w.Write([]byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1})
	return err
}

func (protocolHeader) channel() uint16 {
	panic("only valid as initial handshake")
}

type methodFrame struct {
	ChannelId uint16
	ClassId   uint16
	MethodId  uint16
	Method    message
}

func (f *methodFrame) channel() uint16 { return f.ChannelId }

type heartbeatFrame struct {
	ChannelId uint16
}

func (f *heartbeatFrame) channel() uint16 { return f.ChannelId }


type headerFrame struct {
	ChannelId  uint16
	ClassId    uint16
	weight     uint16
	Size       uint64
	Properties properties
}

func (f *headerFrame) channel() uint16 { return f.ChannelId }

type bodyFrame struct {
	ChannelId uint16
	Body      []byte
}

func (f *bodyFrame) channel() uint16 { return f.ChannelId }
