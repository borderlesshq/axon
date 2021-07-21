package jetstream

import (
	"github.com/Just4Ease/axon"
	"github.com/Just4Ease/axon/codec"
	raw "github.com/Just4Ease/axon/codec/bytes"
	"github.com/Just4Ease/axon/codec/json"
	"github.com/Just4Ease/axon/codec/msgpack"
	"github.com/Just4Ease/axon/messages"
	"github.com/nats-io/nats.go"
	mp "github.com/vmihailenco/msgpack/v5"
)

type stanEvent struct {
	m     *nats.Msg
	msg   messages.Message
	codec map[string]codec.NewCodec
}

func (s stanEvent) Parse(value interface{}) (*messages.Message, error) {
	//nc, ok := s.codec[s.msg.ContentType.String()]
	//if !ok {
	//	return nil, errors.New("unsupported payload codec")
	//}

	//rwc := codec.NewReadWriteCloser(bufferPool)
	////if _, err := rwc.Write(s.msg.Body); err != nil {
	////	return nil, err
	////}
	//
	//
	//cc := nc(rwc)
	//
	//if err := cc.Write(s.msg.Body); err != nil {
	//	return nil, err
	//}
	//
	//fmt.Printf( " Collected message codec %s", cc)
	//
	//
	//if err := cc.Read(value); err != nil {
	//	return nil, err
	//}
	if err := mp.Unmarshal(s.msg.Body, value); err != nil {
		return nil, err
	}

	return &s.msg, nil
}

func (s stanEvent) Ack() {
	_ = s.m.Ack()
}

func (s stanEvent) NAck() {}

func (s stanEvent) Data() []byte {
	return s.m.Data
}

func (s stanEvent) Topic() string {
	return s.m.Subject
}

func newEvent(m *nats.Msg, msg messages.Message) axon.Event {
	return &stanEvent{
		m:   m,
		msg: msg,
		codec: map[string]codec.NewCodec{
			"application/json":         json.NewCodec,
			"application/msgpack":      msgpack.NewCodec,
			"application/octet-stream": raw.NewCodec,
		},
	}
}
