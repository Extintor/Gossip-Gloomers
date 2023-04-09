package common

import (
	"log"
	"strconv"
)

type Message struct {
	Type string `json:"type"`
}

func GetNodeID(nodeName string) uint64 {
	nid, err := strconv.ParseUint(nodeName[1:], 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return nid
}

