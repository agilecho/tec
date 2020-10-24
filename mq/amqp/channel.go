package amqp

import (
	"reflect"
	"sync"
	"sync/atomic"
)

const frameHeaderSize = 1 + 2 + 4 + 1

type Channel struct {
	destructor sync.Once
	m          sync.Mutex
	confirmM   sync.Mutex
	notifyM    sync.RWMutex
	connection *Connection
	rpc       chan message
	consumers *consumers
	id uint16
	closed int32
	noNotify bool
	closes []chan *Error
	flows []chan bool
	returns []chan Return
	cancels []chan string
	confirms   *confirms
	confirming bool
	errors chan *Error
	recv func(*Channel, frame) error
	message messageWithContent
	header  *headerFrame
	body    []byte
}

func newChannel(c *Connection, id uint16) *Channel {
	return &Channel{
		connection: c,
		id:         id,
		rpc:        make(chan message),
		consumers:  makeConsumers(),
		confirms:   newConfirms(),
		recv:       (*Channel).recvMethod,
		errors:     make(chan *Error, 1),
	}
}

func (ch *Channel) shutdown(e *Error) {
	ch.destructor.Do(func() {
		ch.m.Lock()
		defer ch.m.Unlock()

		ch.notifyM.Lock()
		defer ch.notifyM.Unlock()

		if e != nil {
			for _, c := range ch.closes {
				c <- e
			}
		}

		atomic.StoreInt32(&ch.closed, 1)

		if e != nil {
			ch.errors <- e
		}

		ch.consumers.close()

		for _, c := range ch.closes {
			close(c)
		}

		for _, c := range ch.flows {
			close(c)
		}

		for _, c := range ch.returns {
			close(c)
		}

		for _, c := range ch.cancels {
			close(c)
		}

		ch.flows = nil
		ch.closes = nil
		ch.returns = nil
		ch.cancels = nil

		if ch.confirms != nil {
			ch.confirms.Close()
		}

		close(ch.errors)
		ch.noNotify = true
	})
}

func (ch *Channel) send(msg message) (err error) {
	if atomic.LoadInt32(&ch.closed) == 1 {
		return ch.sendClosed(msg)
	}

	return ch.sendOpen(msg)
}

func (ch *Channel) open() error {
	return ch.call(&channelOpen{}, &channelOpenOk{})
}

func (ch *Channel) call(req message, res ...message) error {
	if err := ch.send(req); err != nil {
		return err
	}

	if req.wait() {
		select {
		case e, ok := <-ch.errors:
			if ok {
				return e
			}
			return ErrClosed

		case msg := <-ch.rpc:
			if msg != nil {
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

			return ErrClosed
		}
	}

	return nil
}

func (ch *Channel) sendClosed(msg message) (err error) {
	if _, ok := msg.(*channelCloseOk); ok {
		return ch.connection.send(&methodFrame{
			ChannelId: ch.id,
			Method:    msg,
		})
	}

	return ErrClosed
}

func (ch *Channel) sendOpen(msg message) (err error) {
	if content, ok := msg.(messageWithContent); ok {
		props, body := content.getContent()
		class, _ := content.id()

		var size int
		if ch.connection.Config.FrameSize > 0 {
			size = ch.connection.Config.FrameSize - frameHeaderSize
		} else {
			size = len(body)
		}

		if err = ch.connection.send(&methodFrame{
			ChannelId: ch.id,
			Method:    content,
		}); err != nil {
			return
		}

		if err = ch.connection.send(&headerFrame{
			ChannelId:  ch.id,
			ClassId:    class,
			Size:       uint64(len(body)),
			Properties: props,
		}); err != nil {
			return
		}

		for i, j := 0, size; i < len(body); i, j = j, j+size {
			if j > len(body) {
				j = len(body)
			}

			if err = ch.connection.send(&bodyFrame{
				ChannelId: ch.id,
				Body:      body[i:j],
			}); err != nil {
				return
			}
		}
	} else {
		err = ch.connection.send(&methodFrame{
			ChannelId: ch.id,
			Method:    msg,
		})
	}

	return
}

func (ch *Channel) dispatch(msg message) {
	switch m := msg.(type) {
	case *channelClose:
		ch.m.Lock()
		ch.send(&channelCloseOk{})
		ch.m.Unlock()
		ch.connection.closeChannel(ch, newError(m.ReplyCode, m.ReplyText))
	case *channelFlow:
		ch.notifyM.RLock()
		for _, c := range ch.flows {
			c <- m.Active
		}

		ch.notifyM.RUnlock()
		ch.send(&channelFlowOk{Active: m.Active})
	case *basicCancel:
		ch.notifyM.RLock()
		for _, c := range ch.cancels {
			c <- m.ConsumerTag
		}

		ch.notifyM.RUnlock()
		ch.consumers.cancel(m.ConsumerTag)
	case *basicReturn:
		ret := newReturn(*m)
		ch.notifyM.RLock()
		for _, c := range ch.returns {
			c <- *ret
		}

		ch.notifyM.RUnlock()
	case *basicAck:
		if ch.confirming {
			if m.Multiple {
				ch.confirms.Multiple(Confirmation{m.DeliveryTag, true})
			} else {
				ch.confirms.One(Confirmation{m.DeliveryTag, true})
			}
		}
	case *basicNack:
		if ch.confirming {
			if m.Multiple {
				ch.confirms.Multiple(Confirmation{m.DeliveryTag, false})
			} else {
				ch.confirms.One(Confirmation{m.DeliveryTag, false})
			}
		}
	case *basicDeliver:
		ch.consumers.send(m.ConsumerTag, newDelivery(ch, m))

	default:
		ch.rpc <- msg
	}
}

func (ch *Channel) transition(f func(*Channel, frame) error) error {
	ch.recv = f
	return nil
}

func (ch *Channel) recvMethod(f frame) error {
	switch frame := f.(type) {
	case *methodFrame:
		if msg, ok := frame.Method.(messageWithContent); ok {
			ch.body = make([]byte, 0)
			ch.message = msg
			return ch.transition((*Channel).recvHeader)
		}

		ch.dispatch(frame.Method)

		return ch.transition((*Channel).recvMethod)
	case *headerFrame:
		return ch.transition((*Channel).recvMethod)
	case *bodyFrame:
		return ch.transition((*Channel).recvMethod)
	}

	panic("unexpected frame type")
}

func (ch *Channel) recvHeader(f frame) error {
	switch frame := f.(type) {
	case *methodFrame:
		return ch.recvMethod(f)
	case *headerFrame:
		ch.header = frame

		if frame.Size == 0 {
			ch.message.setContent(ch.header.Properties, ch.body)
			ch.dispatch(ch.message)
			return ch.transition((*Channel).recvMethod)
		}

		return ch.transition((*Channel).recvContent)
	case *bodyFrame:
		return ch.transition((*Channel).recvMethod)
	}

	panic("unexpected frame type")
}

func (ch *Channel) recvContent(f frame) error {
	switch frame := f.(type) {
	case *methodFrame:
		return ch.recvMethod(f)
	case *headerFrame:
		return ch.transition((*Channel).recvMethod)
	case *bodyFrame:
		ch.body = append(ch.body, frame.Body...)

		if uint64(len(ch.body)) >= ch.header.Size {
			ch.message.setContent(ch.header.Properties, ch.body)
			ch.dispatch(ch.message)
			return ch.transition((*Channel).recvMethod)
		}

		return ch.transition((*Channel).recvContent)
	}

	panic("unexpected frame type")
}

func (ch *Channel) Close() error {
	defer ch.connection.closeChannel(ch, nil)
	return ch.call(
		&channelClose{ReplyCode: replySuccess},
		&channelCloseOk{},
	)
}

func (ch *Channel) NotifyClose(c chan *Error) chan *Error {
	ch.notifyM.Lock()
	defer ch.notifyM.Unlock()

	if ch.noNotify {
		close(c)
	} else {
		ch.closes = append(ch.closes, c)
	}

	return c
}

func (ch *Channel) NotifyFlow(c chan bool) chan bool {
	ch.notifyM.Lock()
	defer ch.notifyM.Unlock()

	if ch.noNotify {
		close(c)
	} else {
		ch.flows = append(ch.flows, c)
	}

	return c
}

func (ch *Channel) NotifyReturn(c chan Return) chan Return {
	ch.notifyM.Lock()
	defer ch.notifyM.Unlock()

	if ch.noNotify {
		close(c)
	} else {
		ch.returns = append(ch.returns, c)
	}

	return c
}

func (ch *Channel) NotifyCancel(c chan string) chan string {
	ch.notifyM.Lock()
	defer ch.notifyM.Unlock()

	if ch.noNotify {
		close(c)
	} else {
		ch.cancels = append(ch.cancels, c)
	}

	return c
}

func (ch *Channel) NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64) {
	confirms := ch.NotifyPublish(make(chan Confirmation, cap(ack)+cap(nack)))

	go func() {
		for c := range confirms {
			if c.Ack {
				ack <- c.DeliveryTag
			} else {
				nack <- c.DeliveryTag
			}
		}
		close(ack)
		if nack != ack {
			close(nack)
		}
	}()

	return ack, nack
}

func (ch *Channel) NotifyPublish(confirm chan Confirmation) chan Confirmation {
	ch.notifyM.Lock()
	defer ch.notifyM.Unlock()

	if ch.noNotify {
		close(confirm)
	} else {
		ch.confirms.Listen(confirm)
	}

	return confirm

}

func (ch *Channel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return ch.call(
		&basicQos{
			PrefetchCount: uint16(prefetchCount),
			PrefetchSize:  uint32(prefetchSize),
			Global:        global,
		},
		&basicQosOk{},
	)
}

func (ch *Channel) Cancel(consumer string, noWait bool) error {
	req := &basicCancel{
		ConsumerTag: consumer,
		NoWait:      noWait,
	}
	res := &basicCancelOk{}

	if err := ch.call(req, res); err != nil {
		return err
	}

	if req.wait() {
		ch.consumers.cancel(res.ConsumerTag)
	} else {
		ch.consumers.cancel(consumer)
	}

	return nil
}

func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error) {
	if err := args.Validate(); err != nil {
		return Queue{}, err
	}

	req := &queueDeclare{
		Queue:      name,
		Passive:    false,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		NoWait:     noWait,
		Arguments:  args,
	}
	res := &queueDeclareOk{}

	if err := ch.call(req, res); err != nil {
		return Queue{}, err
	}

	if req.wait() {
		return Queue{
			Name:      res.Queue,
			Messages:  int(res.MessageCount),
			Consumers: int(res.ConsumerCount),
		}, nil
	}

	return Queue{Name: name}, nil
}

func (ch *Channel) QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error) {
	if err := args.Validate(); err != nil {
		return Queue{}, err
	}

	req := &queueDeclare{
		Queue:      name,
		Passive:    true,
		Durable:    durable,
		AutoDelete: autoDelete,
		Exclusive:  exclusive,
		NoWait:     noWait,
		Arguments:  args,
	}
	res := &queueDeclareOk{}

	if err := ch.call(req, res); err != nil {
		return Queue{}, err
	}

	if req.wait() {
		return Queue{
			Name:      res.Queue,
			Messages:  int(res.MessageCount),
			Consumers: int(res.ConsumerCount),
		}, nil
	}

	return Queue{Name: name}, nil
}

func (ch *Channel) QueueInspect(name string) (Queue, error) {
	req := &queueDeclare{
		Queue:   name,
		Passive: true,
	}
	res := &queueDeclareOk{}

	err := ch.call(req, res)

	state := Queue{
		Name:      name,
		Messages:  int(res.MessageCount),
		Consumers: int(res.ConsumerCount),
	}

	return state, err
}

func (ch *Channel) QueueBind(name, key, exchange string, noWait bool, args Table) error {
	if err := args.Validate(); err != nil {
		return err
	}

	return ch.call(
		&queueBind{
			Queue:      name,
			Exchange:   exchange,
			RoutingKey: key,
			NoWait:     noWait,
			Arguments:  args,
		},
		&queueBindOk{},
	)
}

func (ch *Channel) QueueUnbind(name, key, exchange string, args Table) error {
	if err := args.Validate(); err != nil {
		return err
	}

	return ch.call(
		&queueUnbind{
			Queue:      name,
			Exchange:   exchange,
			RoutingKey: key,
			Arguments:  args,
		},
		&queueUnbindOk{},
	)
}

func (ch *Channel) QueuePurge(name string, noWait bool) (int, error) {
	req := &queuePurge{
		Queue:  name,
		NoWait: noWait,
	}
	res := &queuePurgeOk{}

	err := ch.call(req, res)

	return int(res.MessageCount), err
}

func (ch *Channel) QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error) {
	req := &queueDelete{
		Queue:    name,
		IfUnused: ifUnused,
		IfEmpty:  ifEmpty,
		NoWait:   noWait,
	}
	res := &queueDeleteOk{}

	err := ch.call(req, res)

	return int(res.MessageCount), err
}

func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Table) (<-chan Delivery, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	if consumer == "" {
		consumer = uniqueConsumerTag()
	}

	req := &basicConsume{
		Queue:       queue,
		ConsumerTag: consumer,
		NoLocal:     noLocal,
		NoAck:       autoAck,
		Exclusive:   exclusive,
		NoWait:      noWait,
		Arguments:   args,
	}
	res := &basicConsumeOk{}

	deliveries := make(chan Delivery)

	ch.consumers.add(consumer, deliveries)

	if err := ch.call(req, res); err != nil {
		ch.consumers.cancel(consumer)
		return nil, err
	}

	return (<-chan Delivery)(deliveries), nil
}

func (ch *Channel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error {
	if err := args.Validate(); err != nil {
		return err
	}

	return ch.call(
		&exchangeDeclare{
			Exchange:   name,
			Type:       kind,
			Passive:    false,
			Durable:    durable,
			AutoDelete: autoDelete,
			Internal:   internal,
			NoWait:     noWait,
			Arguments:  args,
		},
		&exchangeDeclareOk{},
	)
}

func (ch *Channel) ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error {
	if err := args.Validate(); err != nil {
		return err
	}

	return ch.call(
		&exchangeDeclare{
			Exchange:   name,
			Type:       kind,
			Passive:    true,
			Durable:    durable,
			AutoDelete: autoDelete,
			Internal:   internal,
			NoWait:     noWait,
			Arguments:  args,
		},
		&exchangeDeclareOk{},
	)
}

func (ch *Channel) ExchangeDelete(name string, ifUnused, noWait bool) error {
	return ch.call(
		&exchangeDelete{
			Exchange: name,
			IfUnused: ifUnused,
			NoWait:   noWait,
		},
		&exchangeDeleteOk{},
	)
}

func (ch *Channel) ExchangeBind(destination, key, source string, noWait bool, args Table) error {
	if err := args.Validate(); err != nil {
		return err
	}

	return ch.call(
		&exchangeBind{
			Destination: destination,
			Source:      source,
			RoutingKey:  key,
			NoWait:      noWait,
			Arguments:   args,
		},
		&exchangeBindOk{},
	)
}

func (ch *Channel) ExchangeUnbind(destination, key, source string, noWait bool, args Table) error {
	if err := args.Validate(); err != nil {
		return err
	}

	return ch.call(
		&exchangeUnbind{
			Destination: destination,
			Source:      source,
			RoutingKey:  key,
			NoWait:      noWait,
			Arguments:   args,
		},
		&exchangeUnbindOk{},
	)
}

func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg Publishing) error {
	if err := msg.Headers.Validate(); err != nil {
		return err
	}

	ch.m.Lock()
	defer ch.m.Unlock()

	if err := ch.send(&basicPublish{
		Exchange:   exchange,
		RoutingKey: key,
		Mandatory:  mandatory,
		Immediate:  immediate,
		Body:       msg.Body,
		Properties: properties{
			Headers:         msg.Headers,
			ContentType:     msg.ContentType,
			ContentEncoding: msg.ContentEncoding,
			DeliveryMode:    msg.DeliveryMode,
			Priority:        msg.Priority,
			CorrelationId:   msg.CorrelationId,
			ReplyTo:         msg.ReplyTo,
			Expiration:      msg.Expiration,
			MessageId:       msg.MessageId,
			Timestamp:       msg.Timestamp,
			Type:            msg.Type,
			UserId:          msg.UserId,
			AppId:           msg.AppId,
		},
	}); err != nil {
		return err
	}

	if ch.confirming {
		ch.confirms.Publish()
	}

	return nil
}

func (ch *Channel) Get(queue string, autoAck bool) (msg Delivery, ok bool, err error) {
	req := &basicGet{Queue: queue, NoAck: autoAck}
	res := &basicGetOk{}
	empty := &basicGetEmpty{}

	if err := ch.call(req, res, empty); err != nil {
		return Delivery{}, false, err
	}

	if res.DeliveryTag > 0 {
		return *(newDelivery(ch, res)), true, nil
	}

	return Delivery{}, false, nil
}

func (ch *Channel) Tx() error {
	return ch.call(
		&txSelect{},
		&txSelectOk{},
	)
}

func (ch *Channel) TxCommit() error {
	return ch.call(
		&txCommit{},
		&txCommitOk{},
	)
}

func (ch *Channel) TxRollback() error {
	return ch.call(
		&txRollback{},
		&txRollbackOk{},
	)
}

func (ch *Channel) Flow(active bool) error {
	return ch.call(
		&channelFlow{Active: active},
		&channelFlowOk{},
	)
}

func (ch *Channel) Confirm(noWait bool) error {
	if err := ch.call(
		&confirmSelect{Nowait: noWait},
		&confirmSelectOk{},
	); err != nil {
		return err
	}

	ch.confirmM.Lock()
	ch.confirming = true
	ch.confirmM.Unlock()

	return nil
}

func (ch *Channel) Recover(requeue bool) error {
	return ch.call(
		&basicRecover{Requeue: requeue},
		&basicRecoverOk{},
	)
}

func (ch *Channel) Ack(tag uint64, multiple bool) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	return ch.send(&basicAck{
		DeliveryTag: tag,
		Multiple:    multiple,
	})
}

func (ch *Channel) Nack(tag uint64, multiple bool, requeue bool) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	return ch.send(&basicNack{
		DeliveryTag: tag,
		Multiple:    multiple,
		Requeue:     requeue,
	})
}

func (ch *Channel) Reject(tag uint64, requeue bool) error {
	ch.m.Lock()
	defer ch.m.Unlock()

	return ch.send(&basicReject{
		DeliveryTag: tag,
		Requeue:     requeue,
	})
}
