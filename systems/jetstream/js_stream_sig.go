package jetstream

type streamSig int

const (
	pingSig streamSig = iota + 1
	pongSig
	joinSig
	okSig
	closeSig
)

func (s streamSig) String() string {
	return []string{"ping", "pong", "join", "ok", "close"}[s-1]
}

func (s streamSig) Byte() []byte {
	return []byte(s.String())
}

const (
	streamSuffix    = "-stream"
	heartbeatSuffix = "-heartbeat"
	joinerSuffix    = "-joiner"
	recvSuffix      = "-recv"
)
