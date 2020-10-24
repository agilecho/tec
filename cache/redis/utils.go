package redis

import (
	"bufio"
	"net"
	"strconv"
	"time"
)

func parseLen(p []byte) (int, error) {
	if len(p) == 0 {
		return -1, protocolError("malformed length")
	}

	if p[0] == '-' && len(p) == 2 && p[1] == '1' {
		return -1, nil
	}

	var n int
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return -1, protocolError("illegal bytes in length")
		}
		n += int(b - '0')
	}

	return n, nil
}

func parseInt(p []byte) (interface{}, error) {
	if len(p) == 0 {
		return 0, protocolError("malformed integer")
	}

	var negate bool
	if p[0] == '-' {
		negate = true
		p = p[1:]
		if len(p) == 0 {
			return 0, protocolError("malformed integer")
		}
	}

	var n int64
	for _, b := range p {
		n *= 10
		if b < '0' || b > '9' {
			return 0, protocolError("illegal bytes in length")
		}
		n += int64(b - '0')
	}

	if negate {
		n = -n
	}

	return n, nil
}

func Dial(host, port, password string) (Conn, error) {
	dialer := &net.Dialer {
		KeepAlive: time.Minute * 5,
	}

	netConn, err := dialer.Dial("tcp", host + ":" + port)
	if err != nil {
		panic(err)
	}

	this := &connection {
		conn: netConn,
		bw: bufio.NewWriter(netConn),
		br: bufio.NewReader(netConn),
	}

	if password != "" {
		if _, err := this.Do("AUTH", password); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	return this, nil
}


func Int(reply interface{}) int {
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
	default:
		return 0
	}
}

func Int64(reply interface{}) int64 {
	switch reply := reply.(type) {
	case int64:
		return reply
	case []byte:
		n, _ := strconv.ParseInt(string(reply), 10, 64)
		return n
	default:
		return 0
	}
}

func Uint64(reply interface{}) uint64 {
	switch reply := reply.(type) {
	case int64:
		if reply < 0 {
			return 0
		}

		return uint64(reply)
	case []byte:
		n, _ := strconv.ParseUint(string(reply), 10, 64)
		return n
	default:
		return 0
	}
}

func Float64(reply interface{}) float64 {
	switch reply := reply.(type) {
	case []byte:
		n, _ := strconv.ParseFloat(string(reply), 64)
		return n
	default:
		return 0
	}
}

func String(reply interface{}) string {
	switch reply := reply.(type) {
	case []byte:
		return string(reply)
	case string:
		return reply
	default:
		return ""
	}
}

func sliceHelper(reply interface{}, makeSlice func(int), assign func(int, interface{}) error) {
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

func Strings(reply interface{}) []string {
	var result []string
	sliceHelper(reply, func(n int) { result = make([]string, n) }, func(i int, v interface{}) error {
		switch v := v.(type) {
		case string:
			result[i] = v
			return nil
		case []byte:
			result[i] = string(v)
			return nil
		default:
			return nil
		}
	})

	return result
}

func Bytes(reply interface{}) []byte {
	switch reply := reply.(type) {
	case []byte:
		return reply
	case string:
		return []byte(reply)
	case nil:
		return nil
	case Error:
		return nil
	}
	return nil
}

func Bool(reply interface{}) bool {
	switch reply := reply.(type) {
	case int64:
		return reply != 0
	case []byte:
		result, _ := strconv.ParseBool(string(reply))
		return result
	case nil:
		return false
	case Error:
		return false
	}
	return false
}

func Values(reply interface{}) []interface{} {
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