package jetstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/borderlesshq/axon"
	"github.com/borderlesshq/axon/codec"
	"github.com/borderlesshq/axon/codec/msgpack"
	"github.com/borderlesshq/axon/options"
	"github.com/gookit/color"
	"github.com/nats-io/nats.go"
	"strings"
	"sync"
	"time"
)

const Empty = ""

type eventStore struct {
	opts               options.Options
	nc                 *nats.Conn
	jsc                nats.JetStreamContext
	mu                 *sync.RWMutex
	subscriptions      map[string]*subscription
	publishTopics      map[string]string
	responders         map[string]*responder
	knownSubjectsCount int
	serviceName        string
	jsmEnabled         bool
	msh                codec.Marshaler
}

func (s *eventStore) Close() {

	wg := &sync.WaitGroup{}

	wg.Add(2)
	go func() {
		defer wg.Done()
		for _, sub := range s.subscriptions {
			sub.close()
		}
	}()

	go func() {
		defer wg.Done()
		for _, responder := range s.responders {
			responder.close()
		}
	}()

	wg.Wait()
	s.nc.Close()
}

func (s *eventStore) GetServiceName() string {
	return s.opts.ServiceName
}

func Init(opts ...options.Option) (axon.EventStore, error) {
	opt := options.Options{}
	// Defaults.
	_ = options.SetMarshaler(msgpack.Marshaler{})(&opt)
	_ = options.UseMarshaler()(&opt)
	_ = options.SetContext(context.Background())(&opt)

	for _, o := range opts {
		if err := o(&opt); err != nil {
			return nil, err
		}
	}

	addr := strings.TrimSpace(opt.Address)
	if addr == Empty {
		return nil, axon.ErrInvalidURL
	}

	name := strings.TrimSpace(opt.ServiceName)
	if name == Empty {
		return nil, axon.ErrEmptyStoreName
	}

	opt.ServiceName = strings.TrimSpace(name)
	natsOptions := append(opt.NatOptions(), nats.Name(name))

	if opt.AuthenticationToken != Empty {
		natsOptions = append(natsOptions, nats.Token(opt.AuthenticationToken))
	}

	if opt.Username != Empty || opt.Password != Empty {
		natsOptions = append(natsOptions, nats.UserInfo(opt.Username, opt.Password))
	}

	nc, js, err := connect(opt.ServiceName, opt.Address, natsOptions)
	if err != nil {
		if err == nats.ErrJetStreamNotEnabled {
			goto ignoreError
		}
		return nil, err
	}
ignoreError:
	color.Green.Print("🔥 NATS connected 🚀\n")
	jsmEnabled := false
	if js != nil {
		jsmEnabled = true
		color.Green.Print("🔥 JetStream connected 🚀\n")
	} else {
		color.Green.Print("🌧  JetStream  not connected 💔\n")
	}

	return &eventStore{
		opts:               opt,
		jsc:                js,
		nc:                 nc,
		jsmEnabled:         jsmEnabled,
		serviceName:        name,
		subscriptions:      make(map[string]*subscription),
		responders:         make(map[string]*responder),
		publishTopics:      make(map[string]string),
		knownSubjectsCount: 0,
		msh:                opt.Marshaler(),
		mu:                 &sync.RWMutex{},
	}, nil
}

func (s *eventStore) Run(ctx context.Context, handlers ...axon.EventHandler) {
	for _, handler := range handlers {
		handler.Run()
	}

	s.registerSubjectsOnStream()

	for _, sub := range s.subscriptions {
		go sub.runSubscriptionHandler()
	}

	<-ctx.Done()
}

func (s *eventStore) registerSubjectsOnStream() {
	s.mu.Lock()
	defer s.mu.Unlock()

	var subjects []string
	for topic := range s.subscriptions {
		subjects = append(subjects, topic)
	}

	for _, topic := range s.publishTopics {
		subjects = append(subjects, topic)
	}

	subjects = append(subjects, s.opts.ServiceName)
	// Do not bother altering the stream state if the values are the same.
	if len(subjects) == s.knownSubjectsCount {
		return
	}
	s.knownSubjectsCount = len(subjects)

	if !s.jsmEnabled {
		return
	}

	if _, err := s.jsc.UpdateStream(&nats.StreamConfig{
		Name:     s.opts.ServiceName,
		Subjects: subjects,
		NoAck:    false,
	}); err != nil {
		if strings.Contains(err.Error(), "duplicate subjects detected") {
			streamInfo, _ := s.jsc.StreamInfo(s.opts.ServiceName)
			if len(streamInfo.Config.Subjects) != len(subjects) {
				_ = s.jsc.DeleteStream(s.opts.ServiceName)
				time.Sleep(1 * time.Second)
				streamInfo, _ = s.jsc.AddStream(&nats.StreamConfig{
					Name:     s.opts.ServiceName,
					Subjects: subjects,
					MaxAge:   time.Hour * 48,
					NoAck:    false,
				})
				//PrettyJson(streamInfo)
			}
		}
	}
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}

func connect(sn, addr string, options []nats.Option) (*nats.Conn, nats.JetStreamContext, error) {
	nc, err := nats.Connect(addr, options...)
	if err != nil {
		return nil, nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nc, nil, err
	}

	if _, err = js.StreamInfo(sn); err != nil {
		if strings.Contains(err.Error(), "no responders available for request") {
			return nc, nil, nats.ErrJetStreamNotEnabled
		}

		if err != nats.ErrStreamNotFound {
			return nil, nil, err
		}

		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     sn,
			Subjects: []string{sn},
			NoAck:    false,
		}); err != nil {
			return nil, nil, err
		}
	}

	return nc, js, nil
}
