package jetstream

import (
	"context"
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"log"
	"strings"
	"time"
)

type stream struct {
	executed         bool
	streamChannel    string
	heartBeatChannel string
	receiver         chan []byte
	closer           chan bool
	pipe             *nats.Conn
	timeCreated      time.Time
	streamHandler    axon.StreamHandler
}

func (s *stream) ID() string {
	return s.streamChannel + "_" + s.heartBeatChannel
}

func (s *stream) Send(b []byte) error {
	// DisablePubStreaming ensures we don't store this stream in the eventStore.
	// They are fire and forget.
	if s.closer == nil {
		return errors.New("stream closed")
	}

	return s.pipe.Publish(s.streamChannel, b)
}

func (s *stream) Recv(ctx context.Context) (<-chan []byte, error) {
	out := make(chan []byte)
	errChan := make(chan error)

	go func() {
		<-s.wait()
		close(out)
	}()

	go func() {
		_, err := s.pipe.Subscribe(s.streamChannel, func(msg *nats.Msg) {
			out <- msg.Data
		})

		if err != nil {
			//return nil, err
			errChan <- err
		}
	}()

	ok, err := s.pipe.Request(s.streamChannel+joinerSuffix, joinSig.Byte(), time.Second*1)
	if err != nil {
		s.Close()
		return nil, err
	}

	if string(ok.Data) == closeSig.String() {
		return nil, errors.New("stream closed")
	}

	if string(ok.Data) != okSig.String() {
		_ = ok.Respond(closeSig.Byte())
		return nil, errors.New("stream closed")
	}

	return out, nil
}

func (s *stream) String() string {
	return fmt.Sprintf(`
		ID: %s
		StreamChannel: %s
		HeartBeatChannel: %s
		TimeCreated: %s
	`, s.ID(), s.streamChannel, s.heartBeatChannel, s.timeCreated)
}

func (s *stream) Close() {
	s.executed = false
	if s.closer != nil {
		close(s.closer)
		s.closer = nil
	}
}

func (s *stream) streamFromId(id string) error {
	if s == nil {
		return errors.New("invalid stream")
	}

	split := strings.Split(id, streamSuffix)
	if len(split) != 3 {
		return errors.New("invalid stream")
	}

	s.streamChannel = split[0] + streamSuffix
	s.heartBeatChannel = s.streamChannel + heartbeatSuffix

	return nil
}

func (s *stream) awaitJoiner(processorChan chan<- cmd) {
	defer s.cleanStream(processorChan)
	var sub *nats.Subscription
	var err error
	go func() {
		sub, err = s.pipe.Subscribe(s.streamChannel+joinerSuffix, func(msg *nats.Msg) {
			sig := string(msg.Data)

			if sig != joinSig.String() {
				_ = msg.Respond(closeSig.Byte())
				return
			}

			if s.closer == nil {
				_ = msg.Respond(closeSig.Byte())
				return
			}

			_ = msg.Respond(okSig.Byte())

			processorChan <- cmd{
				flow: serveFlow,
				str:  s,
			}
		})
		if err != nil {
			log.Fatalln(err)
		}
	}()

	<-s.wait()
	if sub != nil {
		_ = sub.Unsubscribe()
	}
}

func (s *stream) cleanStream(processorChan chan<- cmd) {
	// This is used to signal what stream should be cleaned up from the streams.subscribers[]
	if processorChan != nil {
		processorChan <- cmd{
			flow: closeFlow,
			str:  s,
		}
	}
}

func (s *stream) serve() {
	// Check heartbeat of caller if caller is alive.
	go func() {
		defer s.Close()
		for {
			time.Sleep(1 * time.Second) // Check heartbeat every second or die.
			ok, err := s.pipe.Request(s.heartBeatChannel+recvSuffix, pingSig.Byte(), time.Second*1)
			if err != nil {
				break
			}

			if string(ok.Data) != pongSig.String() {
				break
			}
		}
	}()

	// Turn on heart beat for caller so that caller can disconnect if responder is dead.
	go func() {
		// Reply to heartbeat every second or die.
		_, err := s.pipe.Subscribe(s.heartBeatChannel, func(mg *nats.Msg) {
			if string(mg.Data) != pingSig.String() {
				s.Close()
			}

			if err := mg.Respond(pongSig.Byte()); err != nil {
				s.Close()
			}
		})

		if err != nil {
			s.Close()
		}
	}()

	if s.streamHandler != nil {
		s.streamHandler(s.Send, s.Close)
		s.Close()
	}
	// NOTE: defer() func above is called before this function exits.
	//Thus signalling the cleaner to remove this stream from the list of streams.
}

func (s *stream) prepareRecvBeats(processorChan chan<- cmd) {
	defer s.cleanStream(processorChan)
	// Check heartbeat of caller if caller is alive.
	go func() {
		defer s.Close()
		for {
			time.Sleep(1 * time.Second) // Check heartbeat every second or die.
			ok, err := s.pipe.Request(s.heartBeatChannel, pingSig.Byte(), time.Second*1)

			if err != nil {
				break
			}
			if string(ok.Data) != pongSig.String() {
				break
			}
		}
	}()

	go func() {
		_, err := s.pipe.Subscribe(s.heartBeatChannel+recvSuffix, func(mg *nats.Msg) {
			if string(mg.Data) != pingSig.String() {
				s.Close()
			}

			if err := mg.Respond(pongSig.Byte()); err != nil {
				s.Close()
			}
		})

		if err != nil {
			s.Close()
		}
	}()

	<-s.wait()
}

func (s *stream) wait() <-chan bool {
	return s.closer
}
