package jetstream

import (
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/utils"
	"github.com/nats-io/nats.go"
	"sync"
	"time"
)

type flow int

const (
	closeFlow flow = iota + 1
	serveFlow
	joinFow

	// readFlow is used to prepare  stream.Recv for pings.
	readyFlow
)

type cmd struct {
	flow flow
	str  *stream
}

type ncStreams struct {
	su                     sync.RWMutex
	subscribers            map[string]*stream
	streamProcessorChan    chan cmd
	staleStreamCleanupTime time.Duration
	pipe                   *nats.Conn
	sn                     string
}

func (s *ncStreams) JoinStream(id string) (axon.Stream, error) {
	str := &stream{
		executed: true,
		closer:   make(chan bool),
	}
	if err := str.streamFromId(id); err != nil {
		return nil, err
	}

	str = s.add(str, true)

	s.streamProcessorChan <- cmd{
		flow: readyFlow,
		str:  str,
	}

	return str, nil
}

func (s *ncStreams) add(str *stream, skipId bool) *stream {
	s.su.RLock()
	defer s.su.RUnlock()

	str.timeCreated = time.Now()
	str.closer = make(chan bool)
	str.pipe = s.pipe

	if skipId {
		s.subscribers[str.streamChannel] = str
		return str
	}

	streamId := ""
	heartbeatId := ""
kitchen:
	// cook unique id
	streamId = utils.GenerateRandomString() + streamSuffix
	if _, ok := s.subscribers[streamId]; ok {
		goto kitchen // This ensures a regeneration for unique value.
	}

	// produces: id-stream-heartbeat
	heartbeatId = streamId + heartbeatSuffix

	str.streamChannel = streamId
	str.heartBeatChannel = heartbeatId
	s.subscribers[streamId] = str

	return str
}

func (s *ncStreams) NewStream(handler axon.StreamHandler) axon.Stream {
	str := &stream{
		executed:      false,
		streamHandler: handler,
	}

	str = s.add(str, false)
	s.streamProcessorChan <- cmd{
		flow: joinFow,
		str:  str,
	}

	return str
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
			cmd.str = nil
		case serveFlow:
			cmd.str.executed = true
			go cmd.str.serve()
			s.subscribers[cmd.str.streamChannel] = cmd.str // Add it back to the pool.
		case joinFow:
			// We won't mark it as executed, because we want it to be cleaned up.
			go cmd.str.awaitJoiner(s.streamProcessorChan)
		case readyFlow:
			go cmd.str.prepareRecvBeats(s.streamProcessorChan)
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
		duration := time.Now().Add(s.staleStreamCleanupTime)
		if str.timeCreated.After(duration) && !str.executed {
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
		sn:                     s.serviceName,
		streamProcessorChan:    make(chan cmd, 3),
		staleStreamCleanupTime: time.Minute * 5,
		pipe:                   s.nc,
		su:                     sync.RWMutex{},
	}
}
