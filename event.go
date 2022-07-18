package axon

import "github.com/borderlesshq/axon/v2/messages"

type Event interface {
	Ack()
	NAck()
	Message() *messages.Message
	Data() []byte
	Topic() string
}
