package main

import (
	"fmt"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/axon/v2/systems/jetstream"
	"log"
)

func main() {
	ev, err := jetstream.Init(options.SetStoreName("USERS"), options.SetAddress("localhost:4222"))
	if err != nil {
		log.Fatal(err)
	}

	streamer, _ := ev.NewStreamer()
	go streamer.Run()

	stream, err := streamer.JoinStream("01g9anwfnsx23tmktpaxtkjzjb-stream_01g9anwfnsx23tmktpaxtkjzjb-stream-heartbeat")
	if err != nil {
		log.Fatal(err)
	}

	out, err := stream.Recv()

	if err != nil {
		log.Fatal(err)
	}

	for {
		v, ok := <-out
		if !ok {
			break
		}

		fmt.Println(string(v))
	}
}
