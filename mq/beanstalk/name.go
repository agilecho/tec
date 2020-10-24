package beanstalk

import (
	"errors"
)

const NameChars = `\-+/;.$_()0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`

type NameError struct {
	Name string
	Err  error
}

func (e NameError) Error() string {
	return e.Err.Error() + ": " + e.Name
}

func (e NameError) Unwrap() error {
	return e.Err
}

var (
	ErrEmpty   = errors.New("name is empty")
	ErrBadChar = errors.New("name has bad char")
	ErrTooLong = errors.New("name is too long")
)

func checkName(s string) error {
	switch {
	case len(s) == 0:
		return NameError{s, ErrEmpty}
	case len(s) >= 200:
		return NameError{s, ErrTooLong}
	case !containsOnly(s, NameChars):
		return NameError{s, ErrBadChar}
	}
	return nil
}

func containsOnly(s, chars string) bool {
outer:
	for _, c := range s {
		for _, m := range chars {
			if c == m {
				continue outer
			}
		}
		return false
	}
	return true
}
