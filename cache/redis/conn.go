package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

type connection struct {
	mu sync.Mutex
	pending int
	err error
	conn net.Conn
	readTimeout time.Duration
	br *bufio.Reader
	writeTimeout time.Duration
	bw *bufio.Writer
	lenScratch [32]byte
	numScratch [40]byte
}


func (this *connection) Close() error {
	this.mu.Lock()
	err := this.err

	if this.err == nil {
		this.err = errors.New("redis closed")
		err = this.conn.Close()
	}

	this.mu.Unlock()
	return err
}

func (this *connection) fatal(err error) error {
	this.mu.Lock()

	if this.err == nil {
		this.err = err
		this.conn.Close()
	}

	this.mu.Unlock()
	return err
}

func (this *connection) Err() error {
	this.mu.Lock()
	err := this.err
	this.mu.Unlock()
	return err
}

func (this *connection) writeLen(prefix byte, n int) error {
	this.lenScratch[len(this.lenScratch)-1] = '\n'
	this.lenScratch[len(this.lenScratch)-2] = '\r'
	i := len(this.lenScratch) - 3

	for {
		this.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}

	this.lenScratch[i] = prefix

	_, err := this.bw.Write(this.lenScratch[i:])
	return err
}

func (this *connection) writeString(s string) error {
	this.writeLen('$', len(s))
	this.bw.WriteString(s)

	_, err := this.bw.WriteString("\r\n")
	return err
}

func (this *connection) writeBytes(p []byte) error {
	this.writeLen('$', len(p))
	this.bw.Write(p)

	_, err := this.bw.WriteString("\r\n")
	return err
}

func (this *connection) writeInt64(n int64) error {
	return this.writeBytes(strconv.AppendInt(this.numScratch[:0], n, 10))
}

func (this *connection) writeFloat64(n float64) error {
	return this.writeBytes(strconv.AppendFloat(this.numScratch[:0], n, 'g', -1, 64))
}

func (this *connection) writeCommand(cmd string, args []interface{}) error {
	this.writeLen('*', 1 + len(args))

	if err := this.writeString(cmd); err != nil {
		return err
	}

	for i, arg := range args {
		if err := this.writeArg(i, arg, true); err != nil {
			return err
		}
	}

	return nil
}

func (this *connection) writeArg(index int, arg interface{}, argumentTypeOK bool) (err error) {
	switch arg := arg.(type) {
	case string:
		if index == 0 {
			return this.writeString(arg)
		}

		return this.writeString(arg)
	case []byte:
		return this.writeBytes(arg)
	case int:
		return this.writeInt64(int64(arg))
	case int64:
		return this.writeInt64(arg)
	case float64:
		return this.writeFloat64(arg)
	case bool:
		if arg {
			return this.writeString("1")
		} else {
			return this.writeString("0")
		}
	case nil:
		return this.writeString("")
	case Argument:
		if argumentTypeOK {
			return this.writeArg(index, arg.RedisArg(), false)
		}

		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return this.writeBytes(buf.Bytes())
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return this.writeBytes(buf.Bytes())
	}
}

func (this *connection) readLine() ([]byte, error) {
	p, err := this.br.ReadSlice('\n')

	if err == bufio.ErrBufferFull {
		buf := append([]byte{}, p...)

		for err == bufio.ErrBufferFull {
			p, err = this.br.ReadSlice('\n')
			buf = append(buf, p...)
		}

		p = buf
	}

	if err != nil {
		return nil, err
	}

	i := len(p) - 2

	if i < 0 || p[i] != '\r' {
		return nil, protocolError("bad response line terminator")
	}

	return p[:i], nil
}

func (this *connection) readReply() (interface{}, error) {
	line, err := this.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, protocolError("short response line")
	}
	switch line[0] {
	case '+':
		switch string(line[1:]) {
		case "OK":
			return okReply, nil
		case "PONG":
			return pongReply, nil
		default:
			return string(line[1:]), nil
		}
	case '-':
		return Error(string(line[1:])), nil
	case ':':
		return parseInt(line[1:])
	case '$':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		p := make([]byte, n)
		_, err = io.ReadFull(this.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := this.readLine(); err != nil {
			return nil, err
		} else if len(line) != 0 {
			return nil, protocolError("bad bulk string format")
		}
		return p, nil
	case '*':
		n, err := parseLen(line[1:])
		if n < 0 || err != nil {
			return nil, err
		}
		r := make([]interface{}, n)
		for i := range r {
			r[i], err = this.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

func (this *connection) Send(cmd string, args ...interface{}) error {
	this.mu.Lock()
	this.pending += 1
	this.mu.Unlock()

	if this.writeTimeout != 0 {
		this.conn.SetWriteDeadline(time.Now().Add(this.writeTimeout))
	}

	if err := this.writeCommand(cmd, args); err != nil {
		return this.fatal(err)
	}

	return nil
}

func (this *connection) Flush() error {
	if this.writeTimeout != 0 {
		this.conn.SetWriteDeadline(time.Now().Add(this.writeTimeout))
	}

	if err := this.bw.Flush(); err != nil {
		return this.fatal(err)
	}

	return nil
}

func (this *connection) Receive() (interface{}, error) {
	return this.ReceiveWithTimeout(this.readTimeout)
}

func (this *connection) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	var deadline time.Time

	if timeout != 0 {
		deadline = time.Now().Add(timeout)
	}

	this.conn.SetReadDeadline(deadline)

	if reply, err = this.readReply(); err != nil {
		return nil, this.fatal(err)
	}

	this.mu.Lock()

	if this.pending > 0 {
		this.pending -= 1
	}

	this.mu.Unlock()

	if err, ok := reply.(Error); ok {
		return nil, err
	}

	return
}

func (this *connection) Do(cmd string, args ...interface{}) (interface{}, error) {
	this.mu.Lock()
	pending := this.pending
	this.pending = 0
	this.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}

	if this.writeTimeout != 0 {
		this.conn.SetWriteDeadline(time.Now().Add(this.writeTimeout))
	}

	if cmd != "" {
		if err := this.writeCommand(cmd, args); err != nil {
			return nil, this.fatal(err)
		}
	}

	if err := this.bw.Flush(); err != nil {
		return nil, this.fatal(err)
	}

	var deadline time.Time

	this.conn.SetReadDeadline(deadline)

	if cmd == "" {
		reply := make([]interface{}, pending)
		for i := range reply {
			r, e := this.readReply()
			if e != nil {
				return nil, this.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}

	var err error
	var reply interface{}

	for i := 0; i <= pending; i++ {
		var e error

		if reply, e = this.readReply(); e != nil {
			return nil, this.fatal(e)
		}

		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}

	return reply, err
}
