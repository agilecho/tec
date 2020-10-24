package beanstalk

import (
	"time"
)

type TubeSet struct {
	Conn *Conn
	Name map[string]bool
}

func NewTubeSet(c *Conn, name ...string) *TubeSet {
	ts := &TubeSet{c, make(map[string]bool)}
	for _, s := range name {
		ts.Name[s] = true
	}
	return ts
}

func (t *TubeSet) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	r, err := t.Conn.cmd(nil, t, nil, "reserve-with-timeout", dur(timeout))
	if err != nil {
		return 0, nil, err
	}
	body, err = t.Conn.readResp(r, true, "RESERVED %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}
