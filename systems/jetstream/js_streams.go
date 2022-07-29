package jetstream

import (
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/messages"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/axon/v2/utils"
	"github.com/pkg/errors"
	"sync"
	"time"
)

type streamSig int

const (
	pingSig streamSig = iota + 1
	pongSig
	closeSig
)

func (s streamSig) String() string {
	return []string{"ping", "pong", "close"}[s]
}

func (s streamSig) Byte() []byte {
	return []byte(s.String())
}

type flow int

const (
	closeFlow flow = iota + 1
	runFlow
)

type cmd struct {
	flow flow
	str  *stream
}

const (
	streamSuffix    = "-stream"
	heartbeatSuffix = "-heartbeat"
)

type stream struct {
	executed         bool
	streamChannel    string
	heartBeatChannel string
	closer           chan bool
	pipe             *eventStore
	timeCreated      time.Time
	streamHandler    axon.StreamHandler
}

func (s *stream) serve(closeChan chan<- cmd) {
	defer func() {
		// This is used to signal what stream should be cleaned up from the streams.subscribers[]
		closeChan <- cmd{
			flow: closeFlow,
			str:  s,
		}
	}()

	// Check heartbeat of caller if caller is alive.
	go func() {
		defer s.Close()
		for {
			time.Sleep(1 * time.Second) // Check heartbeat every second or die.
			ok, err := s.pipe.Request(s.heartBeatChannel, pingSig.Byte(), options.DisablePubStreaming())
			if err != nil {
				break
			}

			if string(ok.Body) != pongSig.String() {
				break
			}
		}
	}()

	// Turn on heart beat for caller so that caller can disconnect if responder is dead.
	go func() {
		// Reply to heartbeat every second or die.
		err := s.pipe.Reply(s.heartBeatChannel, func(mg *messages.Message) (*messages.Message, error) {
			if string(mg.Body) != pingSig.String() {
				s.Close()
				return mg.WithBody(closeSig.Byte()), nil
			}

			return mg.WithBody(pongSig.Byte()), nil
		})

		if err != nil {
			s.Close()
		}
	}()

	go s.streamHandler(s.Send, s.Close)

	<-s.wait()
	// NOTE: defer() func above is called before this function exits.
	//Thus signalling the cleaner to remove this stream from the list of streams.
}

func (s *stream) ID() string {
	return s.streamChannel + "_" + s.heartBeatChannel
}

func (s *stream) Close() {
	close(s.closer)
}

func (s *stream) wait() <-chan bool {
	return s.closer
}

func (s *stream) Send(b []byte) error {
	// DisablePubStreaming ensures we don't store this stream in the eventStore.
	// They are fire and forget.
	return s.pipe.Publish(s.streamChannel, b, options.DisablePubStreaming())
}

type ncStreams struct {
	su                     sync.RWMutex
	subscribers            map[string]*stream
	streamProcessorChan    chan cmd
	staleStreamCleanupTime time.Duration
	pipe                   *eventStore
}

func (s *ncStreams) NewStream(handler axon.StreamHandler) axon.Stream {
	s.su.RLock()
	streamId := ""
	heartbeatId := ""

kitchen:
	// cook unique id
	streamId = utils.GenerateRandomString() + streamSuffix
	if _, ok := s.subscribers[streamId]; !ok {
		goto kitchen // This ensures a regeneration for unique value.
	}

	// produces: id-stream-heartbeat
	heartbeatId = streamId + heartbeatSuffix
	str := &stream{
		executed:         false,
		streamChannel:    streamId,
		heartBeatChannel: heartbeatId,
		closer:           make(chan bool),
		pipe:             s.pipe,
		timeCreated:      time.Now(),
		streamHandler:    handler,
	}

	s.subscribers[streamId] = str
	s.su.RUnlock()

	// produces: id-stream-heartbeat
	return str
}

func (s *ncStreams) OpenStream(id string) (axon.Stream, error) {
	str, ok := s.subscribers[id]
	if !ok {
		return nil, errors.New("kindly initialize stream setup")
	}

	s.streamProcessorChan <- cmd{
		flow: runFlow,
		str:  str,
	}

	return str, nil
}

func (s *ncStreams) Close() {
	// Range over and close.
	for _, str := range s.subscribers {
		s.streamProcessorChan <- cmd{flow: closeFlow, str: str}
	}
}

func (s *ncStreams) Run() {
	go s.streamCleaner() // Have a runner that cleans up stale streams.

	for cmd := range s.streamProcessorChan {
		s.su.RLock()
		switch cmd.flow {
		case closeFlow:
			delete(s.subscribers, cmd.str.streamChannel)
			if cmd.str.executed {
				cmd.str.Close()
			}
		case runFlow:
			cmd.str.executed = true
			go cmd.str.serve(s.streamProcessorChan)
			s.subscribers[cmd.str.streamChannel] = cmd.str // Add it back to the pool.
		}

		s.su.RUnlock()
	}
}

/**
streamCleaner cleans stale streams.
*/
func (s *ncStreams) streamCleaner() {
roster:
	for _, str := range s.subscribers {
		FiveMinutes := time.Now().Add(s.staleStreamCleanupTime)
		if str.timeCreated.After(FiveMinutes) {
			s.streamProcessorChan <- cmd{
				flow: closeFlow,
				str:  str,
			}
		}
	}

	time.Sleep(time.Second * 1)
	goto roster
}

func (s *eventStore) NewStreamer() axon.Streamer {
	return &ncStreams{
		subscribers:            make(map[string]*stream),
		streamProcessorChan:    make(chan cmd, 3),
		staleStreamCleanupTime: time.Second * 5,
		pipe:                   s,
		su:                     sync.RWMutex{},
	}
}
