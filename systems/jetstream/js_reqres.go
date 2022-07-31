package jetstream

import (
	"errors"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/codec"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/nats-io/nats.go"
	"log"
)

func (s *eventStore) Request(topic string, params []byte, opts ...options.PublisherOption) (*messages.Message, error) {
	option, err := options.DefaultPublisherOptions(opts...)
	if err != nil {
		return nil, err
	}

	message := messages.NewMessage()
	message.WithSubject(topic)
	message.WithBody(params)
	message.Header = option.Headers()
	message.WithType(messages.RequestMessage)
	message.WithSource(s.opts.ServiceName)
	message.WithSpecVersion(option.SpecVersion())
	message.WithContentType(messages.ContentType(option.ContentType()))

	data, err := s.msh.Marshal(message)
	if err != nil {
		return nil, err
	}

	subject := message.Subject + "-" + message.SpecVersion
	msg, err := s.nc.RequestWithContext(option.Context(), subject, data)
	if err != nil {
		return nil, err
	}

	var mg messages.Message
	if err := s.msh.Unmarshal(msg.Data, &mg); err != nil {
		log.Printf("failed to unmarshal reply event into reply struct with the following errors: %v", err)
		_ = msg.Term()
		return nil, err
	}

	_ = msg.Ack()
	return &mg, nil
}

func (s *eventStore) Reply(topic string, handler axon.ReplyHandler, opts ...options.SubscriptionOption) error {
	responderOptions, err := options.DefaultSubOptions(opts...)
	if err != nil {
		return err
	}

	r := &responder{
		topic:         topic,
		responderOpts: responderOptions,
		nc:            s.nc,
		handler:       handler,
		opts:          &s.opts,
		msh:           s.msh,
		closeSignal:   make(chan bool),
	}

	if err := s.registerResponder(r); err != nil {
		return err
	}

	return r.mountResponder()
}

type responder struct {
	topic         string
	responderOpts *options.SubscriptionOptions
	nc            *nats.Conn
	handler       axon.ReplyHandler
	opts          *options.Options
	msh           codec.Marshaler
	closeSignal   chan bool
}

func (r responder) mountResponder() error {
	errChan := make(chan error)

	topic := r.topic + "-" + r.responderOpts.ExpectedSpecVersion()
	go func(errChan chan error) {
		sub, err := r.nc.QueueSubscribe(topic, r.opts.ServiceName, func(msg *nats.Msg) {
			var mg messages.Message
			if err := r.msh.Unmarshal(msg.Data, &mg); err != nil {
				log.Printf("failed to encode reply payload into Message{} with the following error: %v", err)
				errChan <- err
				return
			}

			responseMessage, responseError := r.handler(&mg)
			if responseError != nil {
				responseMessage = messages.NewMessage()
				responseMessage.Error = responseError.Error()
				responseMessage.WithType(messages.ErrorMessage)
			} else {
				responseMessage.WithType(messages.ResponseMessage)
			}
			responseMessage.WithSpecVersion(mg.SpecVersion)
			responseMessage.WithSource(r.opts.ServiceName)
			responseMessage.WithSubject(r.topic)
			responseMessage.WithContentType(messages.ContentType(r.responderOpts.ContentType()))

			data, err := r.msh.Marshal(responseMessage)
			if err != nil {
				log.Print("failed to encode reply payload into []bytes with the following error: ", err)
				errChan <- err
				return
			}

			if err := msg.Respond(data); err != nil {
				log.Print("failed to reply data to the incoming request with the following error: ", err)
				errChan <- err
				return
			}
		})

		if err != nil {
			errChan <- err
			return
		}

		<-r.closeSignal
		if err := sub.Drain(); err != nil {
			errChan <- err
		}
	}(errChan)

	select {
	case err := <-errChan:
		return err
	case <-r.responderOpts.Context().Done():
		r.close()
		return nil
	}
}

func (r responder) close() {
	r.closeSignal <- true
}

func (s *eventStore) registerResponder(responder *responder) error {
	s.mu.Lock()

	err := errors.New("this responder topic has already been used")

	topic := responder.topic + "-" + responder.responderOpts.ExpectedSpecVersion()
	if _, ok := s.publishTopics[topic]; ok {
		s.mu.Unlock()
		return err
	}

	if _, ok := s.subscriptions[topic]; ok {
		s.mu.Unlock()
		return err
	}

	if _, ok := s.responders[topic]; ok {
		s.mu.Unlock()
		return err
	}

	s.responders[topic] = responder
	s.mu.Unlock()

	return nil
}
