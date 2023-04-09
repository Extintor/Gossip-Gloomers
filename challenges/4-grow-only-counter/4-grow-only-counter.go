package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Extintor/gossip-gloomers/internal/common"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddMessage struct {
	Type string `json:"type"`
	Delta int `json:"delta"`
}

type ReadMessage struct {
	Type string `json:"type"`
	Value int `json:"value"`
}

const key string = "counter"

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			v, err := kv.ReadInt(context.Background(), key)
			if err != nil {
				v = 0
				err = kv.CompareAndSwap(context.Background(), key, v, v, true)
			}
			err = kv.CompareAndSwap(context.Background(), key, v, v + body.Delta, true)
			if err != nil {
				break
			}
		}
		
		response := common.Message{Type: "add_ok"}

		return n.Reply(msg, response)
	})
	
	n.Handle("read", func(msg maelstrom.Message) error {
		v, _ := kv.ReadInt(context.Background(), key)
		response := ReadMessage{Type: "read_ok", Value: v}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
