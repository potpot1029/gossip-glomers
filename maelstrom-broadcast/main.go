package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Node struct {
	node            *maelstrom.Node
	topology        map[string][]string
	messages        []int
	seenMessages    map[int]struct{}
	pendingMessages map[string]map[int]time.Time
	mu              sync.RWMutex
}

type BroadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func (n *Node) retryLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		n.mu.Lock()

		now := time.Now()
		for neighbor, messages := range n.pendingMessages {
			for message, retryAt := range messages {
				if now.Before(retryAt) {
					continue
				}

				messages[message] = now.Add(500 * time.Millisecond)

				n.send(neighbor, message)
			}
		}

		n.mu.Unlock()
	}

}

func (n *Node) send(target string, message int) {
	n.node.RPC(target, map[string]any{
		"type":    "broadcast",
		"message": message,
	}, func(msg maelstrom.Message) error {
		n.mu.Lock()
		defer n.mu.Unlock()

		delete(n.pendingMessages[target], message)

		return nil
	})
}

func main() {
	n := &Node{
		node:            maelstrom.NewNode(),
		messages:        make([]int, 0),
		seenMessages:    make(map[int]struct{}),
		pendingMessages: make(map[string]map[int]time.Time),
		topology:        make(map[string][]string),
	}

	hub := "n0"

	n.node.Handle("broadcast", func(msg maelstrom.Message) error {
		var body BroadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		n.mu.Lock()
		if _, ok := n.seenMessages[body.Message]; !ok {
			n.messages = append(n.messages, body.Message)
			n.seenMessages[body.Message] = struct{}{}

			neighbors := make([]string, 0)
			if n.node.ID() == hub {
				neighbors = append(neighbors, n.node.NodeIDs()...)
			} else {
				neighbors = append(neighbors, hub)
			}

			for _, neighbor := range neighbors {
				if neighbor == msg.Src || neighbor == n.node.ID() {
					continue
				}

				if n.pendingMessages[neighbor] == nil {
					n.pendingMessages[neighbor] = make(map[int]time.Time)
				}
				n.pendingMessages[neighbor][body.Message] = time.Now().Add(500 * time.Millisecond)

				n.send(neighbor, body.Message)
			}

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

		n.topology = body.Topology

		resp := map[string]any{
			"type": "topology_ok",
		}

		return n.node.Reply(msg, resp)
	})

	go n.retryLoop()

	if err := n.node.Run(); err != nil {
		log.Fatal(err)
	}
}
