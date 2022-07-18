package main

import (
	"fmt"
	"github.com/borderlesshq/axon/v2/options"
	"github.com/borderlesshq/axon/v2/systems/jetstream"
	"github.com/vmihailenco/msgpack/v5"
	"log"
	"sync"
	"time"
)

func main() {
	ev, err := jetstream.Init(options.SetStoreName("USERS"), options.SetAddress("localhost:4222"))
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	const topic = "test"

	data, err := msgpack.Marshal(struct {
		FirstName string `json:"first_name"`
	}{
		FirstName: "Justice Nefe",
	})

	if err != nil {
		panic(err)
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < 20000; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			if err := ev.Publish(topic, data, options.SetPubContentType("application/msgpack")); err != nil {
				fmt.Print(err, " Error publishing.\n")
			}
		}(wg)
	}
	wg.Wait()
	end := time.Now()

	diff := end.Sub(start)

	fmt.Printf("Start: %s, End: %s, Diff: %s", start, end, diff)
}
