package redis

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Conn interface {
	Close() error
	Err() error
	Do(commandName string, args ...interface{}) (reply interface{}, err error)
	Send(commandName string, args ...interface{}) error
	Flush() error
	Receive() (reply interface{}, err error)
}

type Argument interface {
	RedisArg() interface{}
}

type ConnWithTimeout interface {
	Conn
	DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (reply interface{}, err error)
	ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error)
}

type idleList struct {
	count int
	front, back *poolConn
}

type poolConn struct {
	c Conn
	t time.Time
	created time.Time
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
	return
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


type Pool struct {
	Dial func() (Conn, error)
	TestOnBorrow func(c Conn, t time.Time) error
	MaxIdle int
	MaxActive int
	IdleTimeout time.Duration
	Wait bool
	MaxConnLifetime time.Duration
	chInitialized uint32
	mu sync.Mutex
	closed bool
	active int
	ch chan struct{}
	idle idleList
	waitCount int64
	waitDuration time.Duration
}

func (p *Pool) Get() Conn {
	pc, err := p.get()
	if err != nil {
		return nil
	}

	return &activeConn{p: p, pc: pc}
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
	if atomic.LoadUint32(&p.chInitialized) == 1 {
		return
	}

	p.mu.Lock()
	if p.chInitialized == 0 {
		p.ch = make(chan struct{}, p.MaxActive)
		if p.closed {
			close(p.ch)
		} else {
			for i := 0; i < p.MaxActive; i++ {
				p.ch <- struct{}{}
			}
		}
		atomic.StoreUint32(&p.chInitialized, 1)
	}
	p.mu.Unlock()
}

func (p *Pool) get() (*poolConn, error) {
	var waited time.Duration
	if p.Wait && p.MaxActive > 0 {
		p.lazyInit()

		wait := len(p.ch) == 0
		var start time.Time
		if wait {
			start = time.Now()
		}

		<-p.ch

		if wait {
			waited = time.Since(start)
		}
	}

	p.mu.Lock()

	if waited > 0 {
		p.waitCount++
		p.waitDuration += waited
	}

	if p.IdleTimeout > 0 {
		n := p.idle.count
		for i := 0; i < n && p.idle.back != nil && p.idle.back.t.Add(p.IdleTimeout).Before(time.Now()); i++ {
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
			(p.MaxConnLifetime == 0 || time.Now().Sub(pc.created) < p.MaxConnLifetime) {
			return pc, nil
		}
		pc.c.Close()
		p.mu.Lock()
		p.active--
	}

	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("redigo: get on closed pool")
	}

	if !p.Wait && p.MaxActive > 0 && p.active >= p.MaxActive {
		p.mu.Unlock()
		return nil, errors.New("redigo: connection pool exhausted")
	}

	p.active++
	p.mu.Unlock()
	c, err := p.dial()
	if err != nil {
		c = nil
		p.mu.Lock()
		p.active--
		if p.ch != nil && !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
	}

	return &poolConn{c: c, created: time.Now()}, err
}

func (p *Pool) dial() (Conn, error) {
	return p.Dial()
}

func (p *Pool) put(pc *poolConn, forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		pc.t = time.Now()
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

func (ac *activeConn) Close() error {
	pc := ac.pc
	if pc == nil {
		return nil
	}
	ac.pc = nil

	pc.c.Do("")
	ac.p.put(pc, pc.c.Err() != nil)

	return nil
}

func (ac *activeConn) Err() error {
	pc := ac.pc
	if pc == nil {
		return errors.New("redigo: connection closed")
	}

	return pc.c.Err()
}

func (ac *activeConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errors.New("redigo: connection closed")
	}

	return pc.c.Do(commandName, args...)
}

func (ac *activeConn) DoWithTimeout(timeout time.Duration, commandName string, args ...interface{}) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errors.New("redigo: connection closed")
	}
	cwt, ok := pc.c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}

	return cwt.DoWithTimeout(timeout, commandName, args...)
}

func (ac *activeConn) Send(commandName string, args ...interface{}) error {
	pc := ac.pc
	if pc == nil {
		return errors.New("redigo: connection closed")
	}

	return pc.c.Send(commandName, args...)
}

func (ac *activeConn) Flush() error {
	pc := ac.pc
	if pc == nil {
		return errors.New("redigo: connection closed")
	}

	return pc.c.Flush()
}

func (ac *activeConn) Receive() (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errors.New("redigo: connection closed")
	}

	return pc.c.Receive()
}

func (ac *activeConn) ReceiveWithTimeout(timeout time.Duration) (reply interface{}, err error) {
	pc := ac.pc
	if pc == nil {
		return nil, errors.New("redigo: connection closed")
	}
	cwt, ok := pc.c.(ConnWithTimeout)
	if !ok {
		return nil, errTimeoutNotSupported
	}

	return cwt.ReceiveWithTimeout(timeout)
}