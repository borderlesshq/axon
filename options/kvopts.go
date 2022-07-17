package options

import (
	"errors"
	"time"
)

type KVOptions struct {
	bucketName  string
	description string
	ttl         time.Duration
}

type KVOption func(o *KVOptions) error

func SetBucketName(name string) KVOption {
	return func(o *KVOptions) error {
		o.bucketName = name
		return nil
	}
}

func SetDescription(name string) KVOption {
	return func(o *KVOptions) error {
		o.bucketName = name
		return nil
	}
}

func SetTTL(ttl time.Duration) KVOption {
	return func(o *KVOptions) error {
		if ttl < time.Second {
			return errors.New("unreasonable ttl, use higher values, at least a second")
		}

		o.ttl = ttl
		return nil
	}
}

func (kvo *KVOptions) BucketName() string {
	return kvo.bucketName
}

func (kvo *KVOptions) TTL() time.Duration {
	return kvo.ttl
}

func (kvo *KVOptions) Description() string {
	return kvo.description
}
