package axon

import (
	"context"
	"github.com/borderlesshq/axon/messages"
	"github.com/borderlesshq/axon/options"
	"github.com/pkg/errors"
	"log"
	"time"
)

type SubscriptionHandler func(event Event)
type ReplyHandler func(mg *messages.Message) (*messages.Message, error)
type EventHandler func() error

func (f EventHandler) Run() {
runLabel:
	if err := f(); err != nil {
		log.Printf("creating a consumer returned error: %v. \nRetrying in 3 seconds", err)
		time.Sleep(time.Second * 3)
		goto runLabel
	}
}

var (
	ErrEmptyStoreName          = errors.New("Sorry, you must provide a valid store name")
	ErrInvalidURL              = errors.New("Sorry, you must provide a valid store URL")
	ErrInvalidTlsConfiguration = errors.New("Sorry, you have provided an invalid tls configuration")
	ErrCloseConn               = errors.New("connection closed")
)

type EventStore interface {
	Publish(topic string, data []byte, opts ...options.PublisherOption) error
	Subscribe(topic string, handler SubscriptionHandler, opts ...options.SubscriptionOption) error
	Request(topic string, params []byte, opts ...options.PublisherOption) (*messages.Message, error)
	Reply(topic string, handler ReplyHandler, opts ...options.SubscriptionOption) error
	GetServiceName() string
	Run(ctx context.Context, handlers ...EventHandler)
	NewKVStore(opts ...options.KVOption) (KVStore, error)
	Close()
}

type KVStore interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Del(key string) error
	Purge() error
	Watch(ctx context.Context, key string) (stream <-chan []byte, err error)
	Keys() []string
}
