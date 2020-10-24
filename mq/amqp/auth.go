package amqp

import (
	"fmt"
)

type Authentication interface {
	Mechanism() string
	Response() string
}

type PlainAuth struct {
	Username string
	Password string
}

func (auth *PlainAuth) Mechanism() string {
	return "PLAIN"
}

func (auth *PlainAuth) Response() string {
	return fmt.Sprintf("\000%s\000%s", auth.Username, auth.Password)
}

type AMQPlainAuth struct {
	Username string
	Password string
}

func (auth *AMQPlainAuth) Mechanism() string {
	return "AMQPLAIN"
}

func (auth *AMQPlainAuth) Response() string {
	return fmt.Sprintf("LOGIN:%sPASSWORD:%s", auth.Username, auth.Password)
}

func pickSASLMechanism(client []Authentication, serverMechanisms []string) (auth Authentication, ok bool) {
	for _, auth = range client {
		for _, mech := range serverMechanisms {
			if auth.Mechanism() == mech {
				return auth, true
			}
		}
	}

	return
}
