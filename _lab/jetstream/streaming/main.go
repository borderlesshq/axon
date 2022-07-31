package main

import (
	"fmt"
	"github.com/borderlesshq/axon/v2"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/axon/v2/systems/jetstream"
	"log"
	"time"
)

func main() {
	ev, err := jetstream.Init(options.SetStoreName("USERS"), options.SetAddress("localhost:4222"))
	if err != nil {
		log.Fatal(err)
	}

	streamer, _ := ev.NewStreamer()
	stream := streamer.NewStream(func(send axon.Send, c axon.Close) {

		for {
			time.Sleep(time.Second * 2)

			fmt.Println("send beat...")
			if err := send([]byte("beat")); err != nil {
				log.Print(err)
				break
			}
		}

		return
	})

	fmt.Println(stream)

	streamer.Run()
}
