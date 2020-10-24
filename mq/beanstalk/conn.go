package beanstalk

import (
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strings"
	"time"
)

type Conn struct {
	c       *textproto.Conn
	used    string
	watched map[string]bool
	Tube
	TubeSet
}

var (
	space      = []byte{' '}
	crnl       = []byte{'\r', '\n'}
	yamlHead   = []byte{'-', '-', '-', '\n'}
	nl         = []byte{'\n'}
	colonSpace = []byte{':', ' '}
	minusSpace = []byte{'-', ' '}
)

func NewConn(conn io.ReadWriteCloser) *Conn {
	c := new(Conn)
	c.c = textproto.NewConn(conn)
	c.Tube = *NewTube(c, "default")
	c.TubeSet = *NewTubeSet(c, "default")
	c.used = "default"
	c.watched = map[string]bool{"default": true}
	return c
}

func Dial(network, addr string) (*Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewConn(c), nil
}

func (c *Conn) Close() error {
	return c.c.Close()
}

func (c *Conn) cmd(t *Tube, ts *TubeSet, body []byte, op string, args ...interface{}) (req, error) {
	r := req{c.c.Next(), op}
	c.c.StartRequest(r.id)
	err := c.adjustTubes(t, ts)
	if err != nil {
		return req{}, err
	}
	if body != nil {
		args = append(args, len(body))
	}
	c.printLine(op, args...)
	if body != nil {
		c.c.W.Write(body)
		c.c.W.Write(crnl)
	}
	err = c.c.W.Flush()
	if err != nil {
		return req{}, ConnError{c, op, err}
	}
	c.c.EndRequest(r.id)
	return r, nil
}

func (c *Conn) adjustTubes(t *Tube, ts *TubeSet) error {
	if t != nil && t.Name != c.used {
		if err := checkName(t.Name); err != nil {
			return err
		}
		c.printLine("use", t.Name)
		c.used = t.Name
	}
	if ts != nil {
		for s := range ts.Name {
			if !c.watched[s] {
				if err := checkName(s); err != nil {
					return err
				}
				c.printLine("watch", s)
			}
		}
		for s := range c.watched {
			if !ts.Name[s] {
				c.printLine("ignore", s)
			}
		}
		c.watched = make(map[string]bool)
		for s := range ts.Name {
			c.watched[s] = true
		}
	}
	return nil
}

func (c *Conn) printLine(cmd string, args ...interface{}) {
	io.WriteString(c.c.W, cmd)
	for _, a := range args {
		c.c.W.Write(space)
		fmt.Fprint(c.c.W, a)
	}
	c.c.W.Write(crnl)
}

func (c *Conn) readResp(r req, readBody bool, f string, a ...interface{}) (body []byte, err error) {
	c.c.StartResponse(r.id)
	defer c.c.EndResponse(r.id)
	line, err := c.c.ReadLine()
	for strings.HasPrefix(line, "WATCHING ") || strings.HasPrefix(line, "USING ") {
		line, err = c.c.ReadLine()
	}
	if err != nil {
		return nil, ConnError{c, r.op, err}
	}
	toScan := line
	if readBody {
		var size int
		toScan, size, err = parseSize(toScan)
		if err != nil {
			return nil, ConnError{c, r.op, err}
		}
		body = make([]byte, size+2)
		_, err = io.ReadFull(c.c.R, body)
		if err != nil {
			return nil, ConnError{c, r.op, err}
		}
		body = body[:size]
	}

	err = scan(toScan, f, a...)
	if err != nil {
		return nil, ConnError{c, r.op, err}
	}
	return body, nil
}

func (c *Conn) Delete(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "delete", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "DELETED")
	return err
}

func (c *Conn) Release(id uint64, pri uint32, delay time.Duration) error {
	r, err := c.cmd(nil, nil, nil, "release", id, pri, dur(delay))
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "RELEASED")
	return err
}

func (c *Conn) Bury(id uint64, pri uint32) error {
	r, err := c.cmd(nil, nil, nil, "bury", id, pri)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "BURIED")
	return err
}

func (c *Conn) Touch(id uint64) error {
	r, err := c.cmd(nil, nil, nil, "touch", id)
	if err != nil {
		return err
	}
	_, err = c.readResp(r, false, "TOUCHED")
	return err
}

func (c *Conn) Peek(id uint64) (body []byte, err error) {
	r, err := c.cmd(nil, nil, nil, "peek", id)
	if err != nil {
		return nil, err
	}
	return c.readResp(r, true, "FOUND %d", &id)
}

func (c *Conn) Stats() (map[string]string, error) {
	r, err := c.cmd(nil, nil, nil, "stats")
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseDict(body), err
}

func (c *Conn) StatsJob(id uint64) (map[string]string, error) {
	r, err := c.cmd(nil, nil, nil, "stats-job", id)
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseDict(body), err
}

func (c *Conn) ListTubes() ([]string, error) {
	r, err := c.cmd(nil, nil, nil, "list-tubes")
	if err != nil {
		return nil, err
	}
	body, err := c.readResp(r, true, "OK")
	return parseList(body), err
}

func scan(input, format string, a ...interface{}) error {
	_, err := fmt.Sscanf(input, format, a...)
	if err != nil {
		return findRespError(input)
	}
	return nil
}

type req struct {
	id uint
	op string
}
