package options

import (
	"errors"
	"time"
)

type StreamerOptions struct {
	staleStreamCleanupDuration time.Duration
	streamProcessorSize        int
}

func (o StreamerOptions) ChanSize() int {
	return o.streamProcessorSize
}

func (o StreamerOptions) Duration() time.Duration {
	return o.staleStreamCleanupDuration
}

type StreamerOption func(o *StreamerOptions) error

func SetStaleStreamCleanupDuration(t time.Duration) StreamerOption {
	return func(o *StreamerOptions) error {
		o.staleStreamCleanupDuration = t
		return nil
	}
}

func SetStreamProcessorChanSize(s int) StreamerOption {
	return func(o *StreamerOptions) error {
		if s < 1 {
			return errors.New("stream processor chan size cannot be less than 1")
		}
		o.streamProcessorSize = s
		return nil
	}
}

func DefaultStreamerOptions() StreamerOptions {
	return StreamerOptions{
		staleStreamCleanupDuration: 5 * time.Minute,
		streamProcessorSize:        10,
	}
}
