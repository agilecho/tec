package redis

import (
	"errors"
	"fmt"
)

type Error string

func (err Error) Error() string {
	return string(err)
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

var errTimeoutNotSupported = errors.New("redis: connection does not support ConnWithTimeout")
