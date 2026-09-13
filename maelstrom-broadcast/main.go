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
	Type     string `json:"type"`
	Message  *int   `json:"message,omitempty"`
	Messages []int  `json:"messages,omitempty"`
}

type TopologyBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func (n *Node) retryLoop() {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		batch := make(map[string][]int)

		n.mu.Lock()

		for neighbor, messages := range n.pendingMessages {
			for message, retryAt := range messages {
				if now.Before(retryAt) {
					continue
				}

				batch[neighbor] = append(batch[neighbor], message)
				messages[message] = now.Add(retryInterval)
			}
		}

		n.mu.Unlock()

		for neighbor, messages := range batch {
			n.sendBatch(neighbor, messages)
		}
	}
}

func (n *Node) flushLoop() {
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		batch := make(map[string][]int)

		n.mu.Lock()

		for neighbor, messages := range n.pendingMessages {
			for message, retryAt := range messages {
				if now.Before(retryAt) {
					continue
				}

				batch[neighbor] = append(batch[neighbor], message)
				messages[message] = now.Add(retryInterval)
			}
		}

		n.mu.Unlock()

		for neighbor, messages := range batch {
			n.sendBatch(neighbor, messages)
		}
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

func (n *Node) sendBatch(target string, messages []int) {
	n.node.RPC(target, map[string]any{
		"type":     "broadcast",
		"messages": messages,
	}, func(msg maelstrom.Message) error {
		n.mu.Lock()
		defer n.mu.Unlock()

		for _, message := range messages {
			delete(n.pendingMessages[target], message)
		}

		return nil
	})
}

const (
	flushInterval = 100 * time.Millisecond
	retryInterval = 500 * time.Millisecond
)

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

		messages := make([]int, 0)
		if body.Message != nil {
			messages = append(messages, *body.Message)
		}
		if body.Messages != nil {
			messages = append(messages, body.Messages...)
		}

		n.mu.Lock()
		for _, message := range messages {
			if _, ok := n.seenMessages[message]; !ok {
				n.messages = append(n.messages, message)
				n.seenMessages[message] = struct{}{}

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
					n.pendingMessages[neighbor][message] = time.Now().Add(flushInterval)
				}
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
	go n.flushLoop()

	if err := n.node.Run(); err != nil {
		log.Fatal(err)
	}
}
