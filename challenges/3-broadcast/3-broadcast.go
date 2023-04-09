package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/Extintor/gossip-gloomers/internal/common"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastMessage struct {
	Type string `json:"type"`
	Value int64 `json:"message"`
}

type ReadMessage struct {
	Type string `json:"type"`
	Messages []int64 `json:"messages"`
}

type TopologyMessage struct {
	Type string `json:"type"`
	Topology map[string][]string
}


func main() {
	n := maelstrom.NewNode()

	values := make(map[int64]struct{}, 0)
	topology := make([]string, 0)
	mu := sync.RWMutex{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastMessage 
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		values[body.Value] = struct{}{}
		mu.Unlock()
		response := common.Message{Type: "broadcast_ok"}

		return n.Reply(msg, response)
	})
	
	n.Handle("read", func(msg maelstrom.Message) error {
		var list []int64

		mu.RLock()
		for k := range values {
			list = append(list, k)
		}
		mu.RUnlock()
		response := ReadMessage{Type: "read_ok", Messages: list}

		return n.Reply(msg, response)
	})
	
	n.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyMessage 
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		response := common.Message{Type: "topology_ok"}

		for k := range body.Topology {
			topology = append(topology, k)
		}

		return n.Reply(msg, response)
	})
	
	n.Handle("propagate", func(msg maelstrom.Message) error {
		var body ReadMessage 
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		mu.Lock()
		for _, v := range body.Messages {
			values[v] = struct{}{}
		}
		mu.Unlock()
		return nil
	})

    ticker := time.NewTicker(50 * time.Millisecond)
	round := 0
    done := make(chan bool)

    go func() {
        for {
            select {
            case <-done:
                return
            case <-ticker.C:
				if len(topology) != 0 && uint64(round % len(topology)) == common.GetNodeID(n.ID())   {
					var list []int64
					mu.RLock()
					for k := range values {
						list = append(list, k)
					}
					mu.RUnlock()
					for _, node := range topology {
						n.Send(node, ReadMessage{Type: "propagate", Messages: list})
					}
				}
				round += 1
            }
        }
    }()


	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

    ticker.Stop()
    done <- true

}
