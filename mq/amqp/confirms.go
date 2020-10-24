package amqp

import "sync"

type confirms struct {
	m         sync.Mutex
	listeners []chan Confirmation
	sequencer map[uint64]Confirmation
	published uint64
	expecting uint64
}

func newConfirms() *confirms {
	return &confirms{
		sequencer: map[uint64]Confirmation{},
		published: 0,
		expecting: 1,
	}
}

func (c *confirms) Listen(l chan Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	c.listeners = append(c.listeners, l)
}

func (c *confirms) Publish() uint64 {
	c.m.Lock()
	defer c.m.Unlock()

	c.published++
	return c.published
}

func (c *confirms) confirm(confirmation Confirmation) {
	delete(c.sequencer, c.expecting)
	c.expecting++
	for _, l := range c.listeners {
		l <- confirmation
	}
}

func (c *confirms) resequence() {
	for c.expecting <= c.published {
		sequenced, found := c.sequencer[c.expecting]
		if !found {
			return
		}
		c.confirm(sequenced)
	}
}

func (c *confirms) One(confirmed Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	if c.expecting == confirmed.DeliveryTag {
		c.confirm(confirmed)
	} else {
		c.sequencer[confirmed.DeliveryTag] = confirmed
	}
	c.resequence()
}

func (c *confirms) Multiple(confirmed Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	for c.expecting <= confirmed.DeliveryTag {
		c.confirm(Confirmation{c.expecting, confirmed.Ack})
	}
	c.resequence()
}

func (c *confirms) Close() error {
	c.m.Lock()
	defer c.m.Unlock()

	for _, l := range c.listeners {
		close(l)
	}
	c.listeners = nil
	return nil
}
