package redis

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	_ ConnWithTimeout = (*conn)(nil)
)

type conn struct {
	mu      sync.Mutex
	pending int
	err     error
	conn    net.Conn

	readTimeout time.Duration
	br          *bufio.Reader
	writeTimeout time.Duration
	bw           *bufio.Writer
	lenScratch [32]byte
	numScratch [40]byte
}

type DialOption struct {
	f func(*dialOptions)
}

type dialOptions struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
	dialer       *net.Dialer
	dialContext  func(ctx context.Context, network, addr string) (net.Conn, error)
	db           int
	username     string
	password     string
	clientName   string
	useTLS       bool
	skipVerify   bool
	tlsConfig    *tls.Config
}

func DialReadTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.readTimeout = d
	}}
}

func DialWriteTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.writeTimeout = d
	}}
}

func DialConnectTimeout(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.Timeout = d
	}}
}

func DialKeepAlive(d time.Duration) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialer.KeepAlive = d
	}}
}

func DialNetDial(dial func(network, addr string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dial(network, addr)
		}
	}}
}

func DialContextFunc(f func(ctx context.Context, network, addr string) (net.Conn, error)) DialOption {
	return DialOption{func(do *dialOptions) {
		do.dialContext = f
	}}
}

func DialDatabase(db int) DialOption {
	return DialOption{func(do *dialOptions) {
		do.db = db
	}}
}

func DialPassword(password string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.password = password
	}}
}

func DialUsername(username string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.username = username
	}}
}

func DialClientName(name string) DialOption {
	return DialOption{func(do *dialOptions) {
		do.clientName = name
	}}
}

func DialTLSConfig(c *tls.Config) DialOption {
	return DialOption{func(do *dialOptions) {
		do.tlsConfig = c
	}}
}

func DialTLSSkipVerify(skip bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.skipVerify = skip
	}}
}

func DialUseTLS(useTLS bool) DialOption {
	return DialOption{func(do *dialOptions) {
		do.useTLS = useTLS
	}}
}

func Dial(network, address string, options ...DialOption) (Conn, error) {
	return DialContext(context.Background(), network, address, options...)
}

func DialContext(ctx context.Context, network, address string, options ...DialOption) (Conn, error) {
	fmt.Println(network, address, options)
	do := dialOptions{
		dialer: &net.Dialer{
			KeepAlive: time.Minute * 5,
		},
	}
	for _, option := range options {
		option.f(&do)
	}
	if do.dialContext == nil {
		do.dialContext = do.dialer.DialContext
	}

	netConn, err := do.dialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}

	if do.useTLS {
		var tlsConfig *tls.Config
		if do.tlsConfig == nil {
			tlsConfig = &tls.Config{InsecureSkipVerify: do.skipVerify}
		} else {
			tlsConfig = cloneTLSConfig(do.tlsConfig)
		}
		if tlsConfig.ServerName == "" {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				netConn.Close()
				return nil, err
			}
			tlsConfig.ServerName = host
		}

		tlsConn := tls.Client(netConn, tlsConfig)
		if err := tlsConn.Handshake(); err != nil {
			netConn.Close()
			return nil, err
		}
		netConn = tlsConn
	}

	c := &conn{
		conn:         netConn,
		bw:           bufio.NewWriter(netConn),
		br:           bufio.NewReader(netConn),
		readTimeout:  do.readTimeout,
		writeTimeout: do.writeTimeout,
	}

	if do.password != "" {
		authArgs := make([]interface{}, 0, 2)
		if do.username != "" {
			authArgs = append(authArgs, do.username)
		}
		authArgs = append(authArgs, do.password)
		if _, err := c.Do("AUTH", authArgs...); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	if do.clientName != "" {
		if _, err := c.Do("CLIENT", "SETNAME", do.clientName); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	if do.db != 0 {
		if _, err := c.Do("SELECT", do.db); err != nil {
			netConn.Close()
			return nil, err
		}
	}

	return c, nil
}

func (c *conn) Close() error {
	c.mu.Lock()
	err := c.err
	if c.err == nil {
		c.err = errors.New("redigo: closed")
		err = c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) fatal(err error) error {
	c.mu.Lock()
	if c.err == nil {
		c.err = err
		c.conn.Close()
	}
	c.mu.Unlock()
	return err
}

func (c *conn) Err() error {
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

func (c *conn) writeLen(prefix byte, n int) error {
	c.lenScratch[len(c.lenScratch)-1] = '\n'
	c.lenScratch[len(c.lenScratch)-2] = '\r'
	i := len(c.lenScratch) - 3
	for {
		c.lenScratch[i] = byte('0' + n%10)
		i -= 1
		n = n / 10
		if n == 0 {
			break
		}
	}
	c.lenScratch[i] = prefix
	_, err := c.bw.Write(c.lenScratch[i:])
	return err
}

func (c *conn) writeString(s string) error {
	c.writeLen('$', len(s))
	c.bw.WriteString(s)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeBytes(p []byte) error {
	c.writeLen('$', len(p))
	c.bw.Write(p)
	_, err := c.bw.WriteString("\r\n")
	return err
}

func (c *conn) writeInt64(n int64) error {
	return c.writeBytes(strconv.AppendInt(c.numScratch[:0], n, 10))
}

func (c *conn) writeFloat64(n float64) error {
	return c.writeBytes(strconv.AppendFloat(c.numScratch[:0], n, 'g', -1, 64))
}

func (c *conn) writeCommand(cmd string, args []interface{}) error {
	c.writeLen('*', 1+len(args))
	if err := c.writeString(cmd); err != nil {
		return err
	}
	for _, arg := range args {
		if err := c.writeArg(arg, true); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) writeArg(arg interface{}, argumentTypeOK bool) (err error) {
	switch arg := arg.(type) {
	case string:
		return c.writeString(arg)
	case []byte:
		return c.writeBytes(arg)
	case int:
		return c.writeInt64(int64(arg))
	case int64:
		return c.writeInt64(arg)
	case float64:
		return c.writeFloat64(arg)
	case bool:
		if arg {
			return c.writeString("1")
		} else {
			return c.writeString("0")
		}
	case nil:
		return c.writeString("")
	case Argument:
		if argumentTypeOK {
			return c.writeArg(arg.RedisArg(), false)
		}
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return c.writeBytes(buf.Bytes())
	default:
		var buf bytes.Buffer
		fmt.Fprint(&buf, arg)
		return c.writeBytes(buf.Bytes())
	}
}

type protocolError string

func (pe protocolError) Error() string {
	return fmt.Sprintf("redigo: %s (possible server error or unsupported concurrent read by application)", string(pe))
}

func (c *conn) readLine() ([]byte, error) {
	p, err := c.br.ReadSlice('\n')
	if err == bufio.ErrBufferFull {
		buf := append([]byte{}, p...)
		for err == bufio.ErrBufferFull {
			p, err = c.br.ReadSlice('\n')
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

var (
	okReply   interface{} = "OK"
	pongReply interface{} = "PONG"
)

func (c *conn) readReply() (interface{}, error) {
	line, err := c.readLine()
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
		_, err = io.ReadFull(c.br, p)
		if err != nil {
			return nil, err
		}
		if line, err := c.readLine(); err != nil {
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
			r[i], err = c.readReply()
			if err != nil {
				return nil, err
			}
		}
		return r, nil
	}
	return nil, protocolError("unexpected response line")
}

func (c *conn) Send(cmd string, args ...interface{}) error {
	c.mu.Lock()
	c.pending += 1
	c.mu.Unlock()
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.writeCommand(cmd, args); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *conn) Flush() error {
	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}
	if err := c.bw.Flush(); err != nil {
		return c.fatal(err)
	}
	return nil
}

func (c *conn) Receive() (interface{}, error) {
	return c.ReceiveWithTimeout(c.readTimeout)
}

func (c *conn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	var deadline time.Time
	if timeout != 0 {
		deadline = time.Now().Add(timeout)
	}
	c.conn.SetReadDeadline(deadline)

	if reply, err = c.readReply(); err != nil {
		return nil, c.fatal(err)
	}
	c.mu.Lock()
	if c.pending > 0 {
		c.pending -= 1
	}
	c.mu.Unlock()
	if err, ok := reply.(Error); ok {
		return nil, err
	}
	return
}

func (c *conn) Do(cmd string, args ...interface{}) (interface{}, error) {
	return c.DoWithTimeout(c.readTimeout, cmd, args...)
}

func (c *conn) DoWithTimeout(readTimeout time.Duration, cmd string, args ...interface{}) (interface{}, error) {
	c.mu.Lock()
	pending := c.pending
	c.pending = 0
	c.mu.Unlock()

	if cmd == "" && pending == 0 {
		return nil, nil
	}

	if c.writeTimeout != 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	if cmd != "" {
		if err := c.writeCommand(cmd, args); err != nil {
			return nil, c.fatal(err)
		}
	}

	if err := c.bw.Flush(); err != nil {
		return nil, c.fatal(err)
	}

	var deadline time.Time
	if readTimeout != 0 {
		deadline = time.Now().Add(readTimeout)
	}
	c.conn.SetReadDeadline(deadline)

	if cmd == "" {
		reply := make([]interface{}, pending)
		for i := range reply {
			r, e := c.readReply()
			if e != nil {
				return nil, c.fatal(e)
			}
			reply[i] = r
		}
		return reply, nil
	}

	var err error
	var reply interface{}
	for i := 0; i <= pending; i++ {
		var e error
		if reply, e = c.readReply(); e != nil {
			return nil, c.fatal(e)
		}
		if e, ok := reply.(Error); ok && err == nil {
			err = e
		}
	}
	return reply, err
}
