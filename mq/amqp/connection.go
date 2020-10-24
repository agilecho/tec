package amqp

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxChannelMax = (2 << 15) - 1

	defaultHeartbeat         = 10 * time.Second
	defaultConnectionTimeout = 30 * time.Second
	defaultProduct           = "https://github.com/streadway/amqp"
	defaultVersion           = "β"
	defaultChannelMax = (2 << 10) - 1
	defaultLocale     = "en_US"
)

type Config struct {
	SASL []Authentication
	Vhost string
	ChannelMax int
	FrameSize  int
	Heartbeat  time.Duration
	TLSClientConfig *tls.Config
	Properties Table
	Locale string
	Dial func(network, addr string) (net.Conn, error)
}

type Connection struct {
	destructor sync.Once
	sendM      sync.Mutex
	m          sync.Mutex
	conn io.ReadWriteCloser
	rpc       chan message
	writer    *writer
	sends     chan time.Time
	deadlines chan readDeadliner
	allocator *allocator
	channels  map[uint16]*Channel
	noNotify bool
	closes   []chan *Error
	blocks   []chan Blocking
	errors chan *Error
	Config Config
	Major      int
	Minor      int
	Properties Table
	Locales    []string
	closed int32
}

type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

func DefaultDial(connectionTimeout time.Duration) func(network, addr string) (net.Conn, error) {
	return func(network, addr string) (net.Conn, error) {
		conn, err := net.DialTimeout(network, addr, connectionTimeout)
		if err != nil {
			return nil, err
		}

		if err := conn.SetDeadline(time.Now().Add(connectionTimeout)); err != nil {
			return nil, err
		}

		return conn, nil
	}
}

func Dial(url string) (*Connection, error) {
	return DialConfig(url, Config{
		Heartbeat: defaultHeartbeat,
		Locale:    defaultLocale,
	})
}

func DialConfig(url string, config Config) (*Connection, error) {
	var err error
	var conn net.Conn

	uri, err := ParseURI(url)
	if err != nil {
		return nil, err
	}

	if config.SASL == nil {
		config.SASL = []Authentication{uri.PlainAuth()}
	}

	if config.Vhost == "" {
		config.Vhost = uri.Vhost
	}

	addr := net.JoinHostPort(uri.Host, strconv.FormatInt(int64(uri.Port), 10))

	dialer := config.Dial
	if dialer == nil {
		dialer = DefaultDial(defaultConnectionTimeout)
	}

	conn, err = dialer("tcp", addr)
	if err != nil {
		return nil, err
	}

	if uri.Scheme == "amqps" {
		if config.TLSClientConfig == nil {
			config.TLSClientConfig = new(tls.Config)
		}

		if config.TLSClientConfig.ServerName == "" {
			config.TLSClientConfig.ServerName = uri.Host
		}

		client := tls.Client(conn, config.TLSClientConfig)
		if err := client.Handshake(); err != nil {

			conn.Close()
			return nil, err
		}

		conn = client
	}

	return Open(conn, config)
}

func Open(conn io.ReadWriteCloser, config Config) (*Connection, error) {
	c := &Connection{
		conn:      conn,
		writer:    &writer{bufio.NewWriter(conn)},
		channels:  make(map[uint16]*Channel),
		rpc:       make(chan message),
		sends:     make(chan time.Time),
		errors:    make(chan *Error, 1),
		deadlines: make(chan readDeadliner, 1),
	}
	go c.reader(conn)
	return c, c.open(config)
}

func (c *Connection) LocalAddr() net.Addr {
	if conn, ok := c.conn.(interface {
		LocalAddr() net.Addr
	}); ok {
		return conn.LocalAddr()
	}
	return &net.TCPAddr{}
}

func (c *Connection) ConnectionState() tls.ConnectionState {
	if conn, ok := c.conn.(interface {
		ConnectionState() tls.ConnectionState
	}); ok {
		return conn.ConnectionState()
	}
	return tls.ConnectionState{}
}

func (c *Connection) NotifyClose(receiver chan *Error) chan *Error {
	c.m.Lock()
	defer c.m.Unlock()

	if c.noNotify {
		close(receiver)
	} else {
		c.closes = append(c.closes, receiver)
	}

	return receiver
}

func (c *Connection) NotifyBlocked(receiver chan Blocking) chan Blocking {
	c.m.Lock()
	defer c.m.Unlock()

	if c.noNotify {
		close(receiver)
	} else {
		c.blocks = append(c.blocks, receiver)
	}

	return receiver
}

func (c *Connection) Close() error {
	if c.IsClosed() {
		return ErrClosed
	}

	defer c.shutdown(nil)
	return c.call(
		&connectionClose{
			ReplyCode: replySuccess,
			ReplyText: "kthxbai",
		},
		&connectionCloseOk{},
	)
}

func (c *Connection) closeWith(err *Error) error {
	if c.IsClosed() {
		return ErrClosed
	}

	defer c.shutdown(err)
	return c.call(
		&connectionClose{
			ReplyCode: uint16(err.Code),
			ReplyText: err.Reason,
		},
		&connectionCloseOk{},
	)
}

func (c *Connection) IsClosed() bool {
	return (atomic.LoadInt32(&c.closed) == 1)
}

func (c *Connection) send(f frame) error {
	if c.IsClosed() {
		return ErrClosed
	}

	c.sendM.Lock()
	err := c.writer.WriteFrame(f)
	c.sendM.Unlock()

	if err != nil {
		go c.shutdown(&Error{
			Code:   FrameError,
			Reason: err.Error(),
		})
	} else {
		select {
		case c.sends <- time.Now():
		default:
		}
	}

	return err
}

func (c *Connection) shutdown(err *Error) {
	atomic.StoreInt32(&c.closed, 1)

	c.destructor.Do(func() {
		c.m.Lock()
		defer c.m.Unlock()

		if err != nil {
			for _, c := range c.closes {
				c <- err
			}
		}

		if err != nil {
			c.errors <- err
		}

		close(c.errors)

		for _, c := range c.closes {
			close(c)
		}

		for _, c := range c.blocks {
			close(c)
		}

		for _, ch := range c.channels {
			ch.shutdown(err)
		}

		c.conn.Close()

		c.channels = map[uint16]*Channel{}
		c.allocator = newAllocator(1, c.Config.ChannelMax)
		c.noNotify = true
	})
}

func (c *Connection) demux(f frame) {
	if f.channel() == 0 {
		c.dispatch0(f)
	} else {
		c.dispatchN(f)
	}
}

func (c *Connection) dispatch0(f frame) {
	switch mf := f.(type) {
	case *methodFrame:
		switch m := mf.Method.(type) {
		case *connectionClose:
			c.send(&methodFrame{
				ChannelId: 0,
				Method:    &connectionCloseOk{},
			})

			c.shutdown(newError(m.ReplyCode, m.ReplyText))
		case *connectionBlocked:
			for _, c := range c.blocks {
				c <- Blocking{Active: true, Reason: m.Reason}
			}
		case *connectionUnblocked:
			for _, c := range c.blocks {
				c <- Blocking{Active: false}
			}
		default:
			c.rpc <- m
		}
	case *heartbeatFrame:
	default:
		c.closeWith(ErrUnexpectedFrame)
	}
}

func (c *Connection) dispatchN(f frame) {
	c.m.Lock()
	channel := c.channels[f.channel()]
	c.m.Unlock()

	if channel != nil {
		channel.recv(channel, f)
	} else {
		c.dispatchClosed(f)
	}
}

func (c *Connection) dispatchClosed(f frame) {
	if mf, ok := f.(*methodFrame); ok {
		switch mf.Method.(type) {
		case *channelClose:
			c.send(&methodFrame{
				ChannelId: f.channel(),
				Method:    &channelCloseOk{},
			})
		case *channelCloseOk:
		default:
			c.closeWith(ErrClosed)
		}
	}
}

func (c *Connection) reader(r io.Reader) {
	buf := bufio.NewReader(r)
	frames := &reader{buf}
	conn, haveDeadliner := r.(readDeadliner)

	for {
		frame, err := frames.ReadFrame()

		if err != nil {
			c.shutdown(&Error{Code: FrameError, Reason: err.Error()})
			return
		}

		c.demux(frame)

		if haveDeadliner {
			c.deadlines <- conn
			select {
			case c.deadlines <- conn:
			default:
				//
			}
		}
	}
}

func (c *Connection) heartbeater(interval time.Duration, done chan *Error) {
	const maxServerHeartbeatsInFlight = 3

	var sendTicks <-chan time.Time
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		sendTicks = ticker.C
	}

	lastSent := time.Now()

	for {
		select {
		case at, stillSending := <-c.sends:
			if stillSending {
				lastSent = at
			} else {
				return
			}

		case at := <-sendTicks:
			if at.Sub(lastSent) > interval-time.Second {
				if err := c.send(&heartbeatFrame{}); err != nil {
					return
				}
			}

		case conn := <-c.deadlines:
			if interval > 0 {
				conn.SetReadDeadline(time.Now().Add(maxServerHeartbeatsInFlight * interval))
			}

		case <-done:
			return
		}
	}
}

func (c *Connection) isCapable(featureName string) bool {
	capabilities, _ := c.Properties["capabilities"].(Table)
	hasFeature, _ := capabilities[featureName].(bool)
	return hasFeature
}

func (c *Connection) allocateChannel() (*Channel, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.IsClosed() {
		return nil, ErrClosed
	}

	id, ok := c.allocator.next()
	if !ok {
		return nil, ErrChannelMax
	}

	ch := newChannel(c, uint16(id))
	c.channels[uint16(id)] = ch

	return ch, nil
}

func (c *Connection) releaseChannel(id uint16) {
	c.m.Lock()
	defer c.m.Unlock()

	delete(c.channels, id)
	c.allocator.release(int(id))
}

func (c *Connection) openChannel() (*Channel, error) {
	ch, err := c.allocateChannel()
	if err != nil {
		return nil, err
	}

	if err := ch.open(); err != nil {
		c.releaseChannel(ch.id)
		return nil, err
	}
	return ch, nil
}

func (c *Connection) closeChannel(ch *Channel, e *Error) {
	ch.shutdown(e)
	c.releaseChannel(ch.id)
}

func (c *Connection) Channel() (*Channel, error) {
	return c.openChannel()
}

func (c *Connection) call(req message, res ...message) error {
	if req != nil {
		if err := c.send(&methodFrame{ChannelId: 0, Method: req}); err != nil {
			return err
		}
	}

	select {
	case err, ok := <-c.errors:
		if !ok {
			return ErrClosed
		}
		return err

	case msg := <-c.rpc:
		for _, try := range res {
			if reflect.TypeOf(msg) == reflect.TypeOf(try) {
				vres := reflect.ValueOf(try).Elem()
				vmsg := reflect.ValueOf(msg).Elem()
				vres.Set(vmsg)
				return nil
			}
		}
		return ErrCommandInvalid
	}
}

func (c *Connection) open(config Config) error {
	if err := c.send(&protocolHeader{}); err != nil {
		return err
	}

	return c.openStart(config)
}

func (c *Connection) openStart(config Config) error {
	start := &connectionStart{}

	if err := c.call(nil, start); err != nil {
		return err
	}

	c.Major = int(start.VersionMajor)
	c.Minor = int(start.VersionMinor)
	c.Properties = Table(start.ServerProperties)
	c.Locales = strings.Split(start.Locales, " ")

	auth, ok := pickSASLMechanism(config.SASL, strings.Split(start.Mechanisms, " "))
	if !ok {
		return ErrSASL
	}

	c.Config.SASL = []Authentication{auth}
	c.Config.Locale = config.Locale

	return c.openTune(config, auth)
}

func (c *Connection) openTune(config Config, auth Authentication) error {
	if len(config.Properties) == 0 {
		config.Properties = Table{
			"product": defaultProduct,
			"version": defaultVersion,
		}
	}

	config.Properties["capabilities"] = Table{
		"connection.blocked":     true,
		"consumer_cancel_notify": true,
	}

	ok := &connectionStartOk{
		ClientProperties: config.Properties,
		Mechanism:        auth.Mechanism(),
		Response:         auth.Response(),
		Locale:           config.Locale,
	}
	tune := &connectionTune{}

	if err := c.call(ok, tune); err != nil {
		return ErrCredentials
	}

	c.Config.ChannelMax = pick(config.ChannelMax, int(tune.ChannelMax))
	if c.Config.ChannelMax == 0 {
		c.Config.ChannelMax = defaultChannelMax
	}

	c.Config.ChannelMax = min(c.Config.ChannelMax, maxChannelMax)
	c.Config.FrameSize = pick(config.FrameSize, int(tune.FrameMax))
	c.Config.Heartbeat = time.Second * time.Duration(pick(int(config.Heartbeat/time.Second), int(tune.Heartbeat)))
	go c.heartbeater(c.Config.Heartbeat, c.NotifyClose(make(chan *Error, 1)))

	if err := c.send(&methodFrame{
		ChannelId: 0,
		Method: &connectionTuneOk{
			ChannelMax: uint16(c.Config.ChannelMax),
			FrameMax:   uint32(c.Config.FrameSize),
			Heartbeat:  uint16(c.Config.Heartbeat / time.Second),
		},
	}); err != nil {
		return err
	}

	return c.openVhost(config)
}

func (c *Connection) openVhost(config Config) error {
	req := &connectionOpen{VirtualHost: config.Vhost}
	res := &connectionOpenOk{}

	if err := c.call(req, res); err != nil {
		return ErrVhost
	}

	c.Config.Vhost = config.Vhost

	return c.openComplete()
}

func (c *Connection) openComplete() error {
	if deadliner, ok := c.conn.(interface {
		SetDeadline(time.Time) error
	}); ok {
		_ = deadliner.SetDeadline(time.Time{})
	}

	c.allocator = newAllocator(1, c.Config.ChannelMax)
	return nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func pick(client, server int) int {
	if client == 0 || server == 0 {
		return max(client, server)
	}
	return min(client, server)
}
