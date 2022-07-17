package main

import (
	"github.com/borderlesshq/axon/options"
	"github.com/borderlesshq/axon/systems/jetstream"
	"log"
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

	_ = sampleKV.Set("J", []byte("ssdfsdfsdfdf"))

}
