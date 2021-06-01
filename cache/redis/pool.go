package redis

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"
)

var (
	_ ConnWithTimeout = (*activeConn)(nil)
	_ ConnWithTimeout = (*errorConn)(nil)
)

var nowFunc = time.Now
var ErrPoolExhausted = errors.New("redigo: connection pool exhausted")
var errConnClosed = errors.New("redigo: connection closed")
var errTimeoutNotSupported = errors.New("redis: connection does not support ConnWithTimeout")

type Pool struct {
	Dial func() (Conn, error)
	DialContext func(ctx context.Context) (Conn, error)
	TestOnBorrow func(c Conn, t time.Time) error
	MaxIdle int
	MaxActive int
	IdleTimeout time.Duration
	Wait bool
	MaxConnLifetime time.Duration

	mu sync.Mutex
	closed bool
	active int
	initOnce sync.Once
	ch chan struct{}
	idle idleList
	waitCount int64
	waitDuration time.Duration
}

func (p *Pool) Get() Conn {
	c, _ := p.GetContext(context.Background())
	return c
}

func (p *Pool) GetContext(ctx context.Context) (Conn, error) {
	waited, err := p.waitVacantConn(ctx)
	if err != nil {
		return errorConn{err}, err
	}

	p.mu.Lock()

	if waited > 0 {
		p.waitCount++
		p.waitDuration += waited
	}

	if p.IdleTimeout > 0 {
		n := p.idle.count
		for i := 0; i < n && p.idle.back != nil && p.idle.back.t.Add(p.IdleTimeout).Before(nowFunc()); i++ {
			pc := p.idle.back
			p.idle.popBack()
			p.mu.Unlock()
			pc.c.Close()
			p.mu.Lock()
			p.active--
		}
	}

	for p.idle.front != nil {
		pc := p.idle.front
		p.idle.popFront()
		p.mu.Unlock()
		if (p.TestOnBorrow == nil || p.TestOnBorrow(pc.c, pc.t) == nil) &&
			(p.MaxConnLifetime == 0 || nowFunc().Sub(pc.created) < p.MaxConnLifetime) {
			return &activeConn{p: p, pc: pc}, nil
		}
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	if p.closed {
		p.mu.Unlock()
		err := errors.New("redigo: get on closed pool")
		return errorConn{err}, err
	}

	if !p.Wait && p.MaxActive > 0 && p.active >= p.MaxActive {
		p.mu.Unlock()
		return errorConn{ErrPoolExhausted}, ErrPoolExhausted
	}

	p.active++
	p.mu.Unlock()
	c, err := p.dial(ctx)
	if err != nil {
		p.mu.Lock()
		p.active--
		if p.ch != nil && !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
		return errorConn{err}, err
	}
	return &activeConn{p: p, pc: &poolConn{c: c, created: nowFunc()}}, nil
}

type PoolStats struct {
	ActiveCount int
	IdleCount int
	WaitCount int64
	WaitDuration time.Duration
}

func (p *Pool) Stats() PoolStats {
	p.mu.Lock()
	stats := PoolStats{
		ActiveCount:  p.active,
		IdleCount:    p.idle.count,
		WaitCount:    p.waitCount,
		WaitDuration: p.waitDuration,
	}
	p.mu.Unlock()

	return stats
}

func (p *Pool) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

func (p *Pool) IdleCount() int {
	p.mu.Lock()
	idle := p.idle.count
	p.mu.Unlock()
	return idle
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active -= p.idle.count
	pc := p.idle.front
	p.idle.count = 0
	p.idle.front, p.idle.back = nil, nil
	if p.ch != nil {
		close(p.ch)
	}
	p.mu.Unlock()
	for ; pc != nil; pc = pc.next {
		pc.c.Close()
	}
	return nil
}

func (p *Pool) lazyInit() {
	p.initOnce.Do(func() {
		p.ch = make(chan struct{}, p.MaxActive)
		if p.closed {
			close(p.ch)
		} else {
			for i := 0; i < p.MaxActive; i++ {
				p.ch <- struct{}{}
			}
		}
	})
}

func (p *Pool) waitVacantConn(ctx context.Context) (waited time.Duration, err error) {
	if !p.Wait || p.MaxActive <= 0 {
		return 0, nil
	}

	p.lazyInit()

	wait := len(p.ch) == 0
	var start time.Time
	if wait {
		start = time.Now()
	}

	select {
	case <-p.ch:
		select {
		case <-ctx.Done():
			p.ch <- struct{}{}
			return 0, ctx.Err()
		default:
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if wait {
		return time.Since(start), nil
	}
	return 0, nil
}

func (p *Pool) dial(ctx context.Context) (Conn, error) {
	if p.DialContext != nil {
		return p.DialContext(ctx)
	}
	if p.Dial != nil {
		return p.Dial()
	}
	return nil, errors.New("redigo: must pass Dial or DialContext to pool")
}

func (p *Pool) put(pc *poolConn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		pc.t = nowFunc()
		p.idle.pushFront(pc)
		if p.idle.count > p.MaxIdle {
			pc = p.idle.back
			p.idle.popBack()
		} else {
			pc = nil
		}
	}

	if pc != nil {
		p.mu.Unlock()
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	if p.ch != nil && !p.closed {
		p.ch <- struct{}{}
	}
	p.mu.Unlock()
	return nil
}

type activeConn struct {
	p *Pool
	pc *poolConn
	state int
}

var (
	sentinel []byte
	sentinelOnce sync.Once
)

func initSentinel() {
	p := make([]byte, 64)
	if _, err := rand.Read(p); err == nil {
		sentinel = p
	} else {
		h := sha1.New()
		io.WriteString(h, "Oops, rand failed. Use time instead.")
		io.WriteString(h, strconv.FormatInt(time.Now().UnixNano(), 10))
		sentinel = h.Sum(nil)
	}
}

func (ac *activeConn) firstError(errs ...error) error {
	for _, err := range errs[:len(errs)-1] {
		if err != nil {
			return err
		}
	}
	return errs[len(errs)-1]
}

func (ac *activeConn) Close() (err error) {
	pc := ac.pc
	if pc == nil {
		return nil
	}
	ac.pc = nil

	if ac.state&connectionMultiState != 0 {
		err = pc.c.Send("DISCARD")
		ac.state &^= (connectionMultiState | connectionWatchState)
	} else if ac.state&connectionWatchState != 0 {
		err = pc.c.Send("UNWATCH")
		ac.state &^= connectionWatchState
	}
	if ac.state&connectionSubscribeState != 0 {
		err = ac.firstError(err,
			pc.c.Send("UNSUBSCRIBE"),
			pc.c.Send("PUNSUBSCRIBE"),
		)
		sentinelOnce.Do(initSentinel)
		err = ac.firstError(err,
			pc.c.Send("ECHO", sentinel),
			pc.c.Flush(),
		)
		for {
			p, err2 := pc.c.Receive()
			if err2 != nil {
				err = ac.firstError(err, err2)
				break
			}
			if p, ok := p.([]byte); ok && bytes.Equal(p, sentinel) {
				ac.state &^= connectionSubscribeState
				break
			}
		}
	}
	_, err2 := pc.c.Do("")
	return ac.firstError(
		err,
		err2,
		ac.p.put(pc, ac.state != 0 || pc.c.Err() != nil),
	)
}

func (ac *activeConn) Err() error {
	pc := ac.pc
	if pc == nil {
		return errConnClosed
	}
	return pc.c.Err()
}

func (ac *activeConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	ci := lookupCommandInfo(commandName)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return pc.c.Do(commandName, args...)
}

func (ac *activeConn) DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	cwt, ok := pc.c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	ci := lookupCommandInfo(commandName)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return cwt.DoWithTimeout(timeout, commandName, args...)
}

func (ac *activeConn) Send(commandName string, args ...interface{}) error {
	pc := ac.pc
	if pc == nil {
		return errConnClosed
	}
	ci := lookupCommandInfo(commandName)
	ac.state = (ac.state | ci.Set) &^ ci.Clear
	return pc.c.Send(commandName, args...)
}

func (ac *activeConn) Flush() error {
	pc := ac.pc
	if pc == nil {
		return errConnClosed
	}
	return pc.c.Flush()
}

func (ac *activeConn) Receive() (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	return pc.c.Receive()
}

func (ac *activeConn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errConnClosed
	}
	cwt, ok := pc.c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}
	return cwt.ReceiveWithTimeout(timeout)
}

type errorConn struct{ err error }

func (ec errorConn) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConn) DoWithTimeout(time.Duration, string, ...interface{}) (interface{}, error) {
	return nil, ec.err
}
func (ec errorConn) Send(string, ...interface{}) error                     { return ec.err }
func (ec errorConn) Err() error                                            { return ec.err }
func (ec errorConn) Close() error                                          { return nil }
func (ec errorConn) Flush() error                                          { return ec.err }
func (ec errorConn) Receive() (interface{}, error)                         { return nil, ec.err }
func (ec errorConn) ReceiveWithTimeout(time.Duration) (interface{}, error) { return nil, ec.err }

type idleList struct {
	count       int
	front, back *poolConn
}

type poolConn struct {
	c          Conn
	t          time.Time
	created    time.Time
	next, prev *poolConn
}

func (l *idleList) pushFront(pc *poolConn) {
	pc.next = l.front
	pc.prev = nil
	if l.count == 0 {
		l.back = pc
	} else {
		l.front.prev = pc
	}
	l.front = pc
	l.count++
}

func (l *idleList) popFront() {
	pc := l.front
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.next.prev = nil
		l.front = pc.next
	}
	pc.next, pc.prev = nil, nil
}

func (l *idleList) popBack() {
	pc := l.back
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.prev.next = nil
		l.back = pc.prev
	}
	pc.next, pc.prev = nil, nil
}
