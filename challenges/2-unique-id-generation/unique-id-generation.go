package main

import (
	"encoding/binary"
	"log"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func getNodeID(nodeName string) uint64 {
	nid, err := strconv.ParseUint(nodeName[1:], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return nid
}

func buildSnowflake(nodeID string, counter uint64) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint64(b, uint64(time.Now().UnixNano()))
	binary.LittleEndian.AppendUint64(b, getNodeID(nodeID))
	binary.LittleEndian.AppendUint64(b, counter)
	return b 
}

func main() {
	var counter uint64

	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		counter += 1
		body := make(map[string]any)

		body["type"] = "generate_ok"
		body["id"] = buildSnowflake(n.ID(), counter) 
 
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
