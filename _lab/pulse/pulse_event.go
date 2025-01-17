package pulse

import (
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/messages"
)

type event struct {
	raw      Message
	consumer Consumer
}

func (e *event) NAck() {
	e.consumer.Ack()
}

func (e *event) Parse(value interface{}) (*messages.Message, error) {
	panic("implement me")
}

func NewEvent(message Message, consumer Consumer) axon.Event {
	return &event{raw: message, consumer: consumer}
}

func (e *event) Data() []byte {
	return e.raw.Payload()
}

func (e *event) Topic() string {
	t := e.raw.Topic()
	// Manually agree what we want the topic to look like from pulsar.
	return t
}

func (e *event) Ack() {
	e.consumer.Ack(e.raw.ID())
}
