package main

import (
	"context"
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

	streamer := ev.NewStreamer()
	go streamer.Run()

	stream, err := streamer.JoinStream("01g9anwfnsx23tmktpaxtkjzjb-stream_01g9anwfnsx23tmktpaxtkjzjb-stream-heartbeat")
	if err != nil {
		log.Fatal(err)
	}

	out, err := stream.Recv(context.Background())

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
