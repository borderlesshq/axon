package options

import (
	"context"
	"errors"
	"github.com/borderlesshq/axon/v2/codec"
	"github.com/nats-io/nats.go"
	"strings"
)

type Options struct {
	// Used to select codec
	ServiceName         string
	Address             string
	CertContent         string
	AuthenticationToken string
	Username            string
	Password            string
	msh                 codec.Marshaler
	ctx                 context.Context
	codec               codec.Codec
	useMarshaler        bool
	useCodec            bool
	natsOptions         []nats.Option
	// Other options for implementations of the interface
}

type Option func(o *Options) error

func SetStoreName(name string) Option {
	return func(o *Options) error {
		sn := strings.TrimSpace(name)
		if sn == "" {
			return errors.New("invalid store name")
		}
		o.ServiceName = sn
		return nil
	}
}

func SetAddress(address string) Option {
	return func(o *Options) error {
		o.Address = address
		return nil
	}
}

func SetAuthorizationToken(token string) Option {
	return func(o *Options) error {
		o.AuthenticationToken = token
		return nil
	}
}

func SetUsername(username string) Option {
	return func(o *Options) error {
		o.Username = username
		return nil
	}
}

func SetPassword(password string) Option {
	return func(o *Options) error {
		o.Password = password
		return nil
	}
}

func SetContext(ctx context.Context) Option {
	return func(o *Options) error {
		if ctx == nil {
			return errors.New("invalid context")
		}

		o.ctx = ctx
		return nil
	}
}

func SetMarshaler(msh codec.Marshaler) Option {
	return func(o *Options) error {
		if msh == nil {
			return errors.New("invalid marshaler")
		}

		o.msh = msh
		return nil
	}
}

func SetCodec(c codec.Codec) Option {
	return func(o *Options) error {
		if c == nil {
			return errors.New("invalid codec")
		}

		o.codec = c
		return nil
	}
}

func SetNatsOptions(opts ...nats.Option) Option {
	return func(o *Options) error {
		if len(o.natsOptions) == 0 {
			o.natsOptions = opts
		} else {
			o.natsOptions = append(o.natsOptions, opts...)
		}

		return nil
	}
}

func UseMarshaler() Option {
	return func(o *Options) error {
		if o.msh == nil {
			return errors.New("cannot use marshaler, marshaler not provided in the options. see options.SetMarshaler()")
		}

		o.useMarshaler = true
		o.useCodec = false

		return nil
	}
}

func UseCodec() Option {
	return func(o *Options) error {
		if o.codec == nil {
			return errors.New("cannot use codec, codec not provided in the options. see options.SetCodec()")
		}

		o.useCodec = true
		o.useMarshaler = false

		return nil
	}
}

func (o *Options) Context() context.Context {
	if o.ctx == nil {
		return context.Background()
	}

	return o.ctx
}

func (o *Options) Marshaler() codec.Marshaler {
	if o.useMarshaler {
		return o.msh
	}
	return nil
}

func (o *Options) Codec() codec.Codec {
	if o.useCodec {
		return o.codec
	}
	return nil
}

func (o *Options) NatOptions() []nats.Option {
	return o.natsOptions
}
