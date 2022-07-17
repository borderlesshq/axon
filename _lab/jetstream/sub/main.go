package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/borderlesshq/axon"
	"github.com/borderlesshq/axon/options"
	"github.com/borderlesshq/axon/systems/jetstream"
	"log"
)

func main() {

	name := flag.String("name", "", "help message for flagname")
	flag.Parse()

	ev, err := jetstream.Init(options.SetStoreName(*name), options.SetAddress("localhost:4222"))

	if err != nil {
		log.Fatal(err)
	}

	handleSubEv := func() error {
		const topic = "test"
		return ev.Subscribe(topic, func(event axon.Event) {
			defer event.Ack()

			PrettyJson(event.Message())
		})
	}

	ev.Run(context.Background(), handleSubEv)
}

const (
	empty = ""
	tab   = "\t"
)

func PrettyJson(data interface{}) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.SetIndent(empty, tab)

	err := encoder.Encode(data)
	if err != nil {
		return
	}
	fmt.Print(buffer.String())
}
