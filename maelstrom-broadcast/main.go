package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	node         *maelstrom.Node
	messages     []int
	seenMessages map[int]bool
	mu           sync.RWMutex
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := &Node{
		node:         maelstrom.NewNode(),
		messages:     make([]int, 0),
		seenMessages: make(map[int]bool),
	}

	n.node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		n.mu.Lock()
		if !n.seenMessages[body.Message] {
			n.messages = append(n.messages, body.Message)

			for _, neighbor := range n.node.NodeIDs() {
				if neighbor == msg.Src || neighbor == n.node.ID() {
					continue
				}

				n.node.Send(neighbor, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
				})
			}
			n.seenMessages[body.Message] = true
		}
		n.mu.Unlock()

		resp := map[string]any{
			"type": "broadcast_ok",
		}

		return n.node.Reply(msg, resp)

	})

	n.node.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		n.mu.RLock()
		defer n.mu.RUnlock()

		resp := map[string]any{
			"type":     "read_ok",
			"messages": n.messages,
		}

		return n.node.Reply(msg, resp)
	})

	n.node.Handle("topology", func(msg maelstrom.Message) error {
		var body TopologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		resp := map[string]any{
			"type": "topology_ok",
		}

		return n.node.Reply(msg, resp)
	})

	if err := n.node.Run(); err != nil {
		log.Fatal(err)
	}
}
