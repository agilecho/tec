package redis

import (
	"crypto/tls"
	"fmt"
	"strconv"
)

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

func Values(reply interface{}, err error) []interface{} {
	if err != nil {
		return nil
	}
	switch reply := reply.(type) {
	case []interface{}:
		return reply
	case nil:
		return nil
	case Error:
		return nil
	}
	return nil
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

func Int64s(reply interface{}, err error) ([]int64, error) {
	var result []int64
	sliceHelper(reply, err, "Int64s", func(n int) { result = make([]int64, n) }, func(i int, v interface{}) error {
		switch v := v.(type) {
		case int64:
			result[i] = v
			return nil
		case []byte:
			n, err := strconv.ParseInt(string(v), 10, 64)
			result[i] = n
			return err
		default:
			return fmt.Errorf("redigo: unexpected element type for Int64s, got type %T", v)
		}
	})
	return result, err
}

func Ints(reply interface{}, err error) ([]int, error) {
	var result []int
	sliceHelper(reply, err, "Ints", func(n int) { result = make([]int, n) }, func(i int, v interface{}) error {
		switch v := v.(type) {
		case int64:
			n := int(v)
			if int64(n) != v {
				return strconv.ErrRange
			}
			result[i] = n
			return nil
		case []byte:
			n, err := strconv.Atoi(string(v))
			result[i] = n
			return err
		default:
			return fmt.Errorf("redigo: unexpected element type for Ints, got type %T", v)
		}
	})
	return result, err
}

func cloneTLSConfig(cfg *tls.Config) *tls.Config {
	return cfg.Clone()
}