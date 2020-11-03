package redis

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strconv"
)

var ErrNil = errors.New("redigo: nil returned")

func Int(reply interface{}, err error) int {
	if err != nil {
		return 0
	}
	switch reply := reply.(type) {
	case int64:
		x := int(reply)
		if int64(x) != reply {
			return 0
		}
		return x
	case []byte:
		n, _ := strconv.ParseInt(string(reply), 10, 0)
		return int(n)
	case nil:
		return 0
	case Error:
		return 0
	}
	return 0
}


func String(reply interface{}, err error) string {
	if err != nil {
		return ""
	}
	switch reply := reply.(type) {
	case []byte:
		return string(reply)
	case string:
		return reply
	case nil:
		return ""
	case Error:
		return ""
	}
	return ""
}

func sliceHelper(reply interface{}, err error, name string, makeSlice func(int), assign func(int, interface{}) error) {
	if err != nil {
		return
	}

	switch reply := reply.(type) {
	case []interface{}:
		makeSlice(len(reply))
		for i := range reply {
			if reply[i] == nil {
				continue
			}
			if err := assign(i, reply[i]); err != nil {
				return
			}
		}
	}
}

func Strings(reply interface{}, err error) []string {
	var result []string
	sliceHelper(reply, err, "Strings", func(n int) { result = make([]string, n) }, func(i int, v interface{}) error {
		switch v := v.(type) {
		case string:
			result[i] = v
			return nil
		case []byte:
			result[i] = string(v)
			return nil
		default:
			return fmt.Errorf("redigo: unexpected element type for Strings, got type %T", v)
		}
	})

	return result
}

func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	return cfg.Clone()
}