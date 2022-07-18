package main

import (
	"context"
	"fmt"
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

	sampleKV, err := ev.NewKVStore(options.SetBucketName("users"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, canc := context.WithTimeout(context.Background(), time.Minute*1)
	defer canc()
	out, err := sampleKV.Watch(ctx, "J")
	if err != nil {
		log.Fatal(err)
	}

	for {
		fmt.Println(string(<-out))
	}
}
