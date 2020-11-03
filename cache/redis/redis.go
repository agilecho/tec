package redis

import (
	"time"
)

type Error string

func (err Error) Error() string {
	return string(err)
}

type Conn interface {
	Close() error
	Err() error
	Do(commandName string, args ...interface{}) (reply interface{}, err error)
	Send(commandName string, args ...interface{}) error
	Flush() error
	Receive() (reply interface{}, err error)
}

type Argument interface {
	RedisArg() interface{}
}

type Scanner interface {
	RedisScan(src interface{}) error
}

type ConnWithTimeout interface {
	Conn
	DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (reply interface{}, err error)
	ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error)
}
