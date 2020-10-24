package amqp

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
)

var consumerSeq uint64

const consumerTagLengthMax = 0xFF

func uniqueConsumerTag() string {
	return commandNameBasedUniqueConsumerTag(os.Args[0])
}

func commandNameBasedUniqueConsumerTag(commandName string) string {
	tagPrefix := "ctag-"
	tagInfix := commandName
	tagSuffix := "-" + strconv.FormatUint(atomic.AddUint64(&consumerSeq, 1), 10)

	if len(tagPrefix)+len(tagInfix)+len(tagSuffix) > consumerTagLengthMax {
		tagInfix = "streadway/amqp"
	}

	return tagPrefix + tagInfix + tagSuffix
}

type consumerBuffers map[string]chan *Delivery

type consumers struct {
	sync.WaitGroup
	closed chan struct{}

	sync.Mutex
	chans      consumerBuffers
}

func makeConsumers() *consumers {
	return &consumers{
		closed: make(chan struct{}),
		chans:  make(consumerBuffers),
	}
}

func (subs *consumers) buffer(in chan *Delivery, out chan Delivery) {
	defer close(out)
	defer subs.Done()

	var inflight = in
	var queue []*Delivery

	for delivery := range in {
		queue = append(queue, delivery)

		for len(queue) > 0 {
			select {
			case <-subs.closed:
				return

			case delivery, consuming := <-inflight:
				if consuming {
					queue = append(queue, delivery)
				} else {
					inflight = nil
				}

			case out <- *queue[0]:
				queue = queue[1:]
			}
		}
	}
}

func (subs *consumers) add(tag string, consumer chan Delivery) {
	subs.Lock()
	defer subs.Unlock()

	if prev, found := subs.chans[tag]; found {
		close(prev)
	}

	in := make(chan *Delivery)
	subs.chans[tag] = in

	subs.Add(1)
	go subs.buffer(in, consumer)
}

func (subs *consumers) cancel(tag string) (found bool) {
	subs.Lock()
	defer subs.Unlock()

	ch, found := subs.chans[tag]

	if found {
		delete(subs.chans, tag)
		close(ch)
	}

	return found
}

func (subs *consumers) close() {
	subs.Lock()
	defer subs.Unlock()

	close(subs.closed)

	for tag, ch := range subs.chans {
		delete(subs.chans, tag)
		close(ch)
	}

	subs.Wait()
}

func (subs *consumers) send(tag string, msg *Delivery) bool {
	subs.Lock()
	defer subs.Unlock()

	buffer, found := subs.chans[tag]
	if found {
		buffer <- msg
	}

	return found
}
