package axon

import "github.com/borderlesshq/axon/messages"

type Event interface {
	Ack()
	NAck()
	Message() *messages.Message
	Data() []byte
	Topic() string
}
