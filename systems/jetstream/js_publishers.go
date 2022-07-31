package jetstream

import (
	"errors"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/axon/v2/options"
	"strings"
)

func (s *eventStore) Publish(topic string, data []byte, opts ...options.PublisherOption) error {
	if strings.TrimSpace(topic) == empty {
		return errors.New("invalid topic name")
	}

	option, err := options.DefaultPublisherOptions(opts...)
	if err != nil {
		return err
	}

	message := messages.NewMessage()
	message.WithSubject(topic)
	message.WithBody(data)
	message.WithType(messages.EventMessage)
	message.WithSource(s.opts.ServiceName)
	message.WithSpecVersion(option.SpecVersion())
	message.WithContentType(messages.ContentType(option.ContentType()))
	message.Header = option.Headers()

	d, err := s.msh.Marshal(message)
	if err != nil {
		return err
	}

	subject := topic + "-" + option.SpecVersion()

	// Publish using NATS connection if JetStream is not enabled on nats-server.
	if option.IsStreamingDisabled() || !s.jsmEnabled {
		return s.nc.Publish(subject, d)
	}

	s.mountAndRegisterPublishTopics(subject)
	_, err = s.jsc.Publish(subject, d)
	return err
}

func (s *eventStore) mountAndRegisterPublishTopics(topic string) {
	s.mu.Lock()
	if _, ok := s.publishTopics[topic]; ok {
		s.mu.Unlock()
		return
	}

	if _, ok := s.subscriptions[topic]; ok {
		s.mu.Unlock()
		return
	}

	s.publishTopics[topic] = topic
	s.mu.Unlock()

	s.registerSubjectsOnStream()
}
