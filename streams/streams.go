package streams

import (
	"errors"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/utils"
	"time"
)

func (s *Streams) RunStream(channel string) (heatBeatChannel string, err error) {
	str, ok := s.subscribers[channel]
	if !ok {
		return "", errors.New("kindly initialize stream setup")
	}

	s.streamProcessorChan <- cmd{
		flow: serveFlow,
		str:  str,
	}

	return str.heartBeatChannel, nil
}

func (s *Streams) NewStream(h axon.StreamHandler) (streamChannel, heartBeatChannel string) {
	s.su.RLock()

kitchen:
	// Cook unique id
	streamChannel = utils.GenerateRandomString() + streamSuffix
	if _, ok := s.subscribers[streamChannel]; !ok {
		goto kitchen // This ensures a regeneration for unique value.
	}

	// produces: id-stream-heartbeat
	heartBeatChannel = streamChannel + heartbeatSuffix

	s.subscribers[streamChannel] = &stream{
		executed:         false,
		streamChannel:    streamChannel,
		heartBeatChannel: heartBeatChannel,
		closer:           make(chan bool),
		pipe:             s.pipe,
		timeCreated:      time.Now(),
		streamHandler:    h,
	}
	s.su.RUnlock()

	return streamChannel, heartBeatChannel
}
