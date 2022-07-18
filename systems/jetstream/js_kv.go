package jetstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/nats-io/nats.go"
	"time"
)

var (
	ErrJetstreamServerRequired = errors.New("jetstream must be connected to a jetstream enabled nats-server")
	ErrJetstreamMustBeEnabled  = errors.New("jetstream must be enabled on this axon client")
)

type kvStore struct {
	kv nats.KeyValue
}

func (k kvStore) Get(key string) ([]byte, error) {
	entry, err := k.kv.Get(key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (k kvStore) Set(key string, value []byte) error {
	_, err := k.kv.Put(key, value)
	return err
}

func (k kvStore) Del(key string) error {
	return k.kv.Delete(key)
}

func (k kvStore) Purge() error {
	return k.kv.PurgeDeletes()
}

func (k kvStore) Keys() []string {
	l, _ := k.kv.Keys()
	return l
}

func (k kvStore) Watch(ctx context.Context, key string) (<-chan []byte, error) {
	kw, err := k.kv.Watch(key, nats.IgnoreDeletes())
	if err != nil {
		return nil, err
	}
	stream := make(chan []byte)
	go func() {
		for {
			select {
			case entry := <-kw.Updates():
				if entry != nil {
					stream <- entry.Value()
				}
				break
			case <-ctx.Done():
				_ = kw.Stop()
				break
			}
		}
	}()

	return stream, nil
}

func (s *eventStore) NewKVStore(opts ...options.KVOption) (axon.KVStore, error) {
	if s.jsc == nil {
		return nil, ErrJetstreamServerRequired
	}

	if !s.jsmEnabled {
		return nil, ErrJetstreamMustBeEnabled
	}

	opt := &options.KVOptions{}
	_ = options.SetTTL(10 * time.Minute)(opt)
	for _, o := range opts {
		if err := o(opt); err != nil {
			return nil, err
		}
	}

	if opt.BucketName() == "" {
		return nil, errors.New("invalid kv bucket name")
	}

	bucket := fmt.Sprintf("%s-%s", s.opts.ServiceName, opt.BucketName())
	kv, err := s.jsc.KeyValue(bucket)
checkErr:
	if err != nil {
		if err == nats.ErrBucketNotFound {
			kv, err = s.jsc.CreateKeyValue(&nats.KeyValueConfig{
				Bucket:      bucket,
				Description: opt.Description(),
				History:     0,
				TTL:         opt.TTL(),
				Replicas:    len(s.nc.Servers()),
			})
			goto checkErr
		}

		return nil, err
	}

	return &kvStore{kv: kv}, nil
}
