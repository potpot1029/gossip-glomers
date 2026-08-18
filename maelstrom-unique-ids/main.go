package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generateSalt(numBytes int) ([]byte, error) {
	salt := make([]byte, numBytes)

	_, err := rand.Read(salt)
	if err != nil {
		return nil, err
	}

	return salt, err
}

func generateUniqueID(nodeID string) (string, error) {
	timestamp := time.Now().UnixMilli()
	salt, err := generateSalt(4)
	if err != nil {
		return "", err
	}
	saltString := base64.StdEncoding.EncodeToString(salt)

	return fmt.Sprintf("%s-%d-%s", nodeID, timestamp, saltString), err
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		id, err := generateUniqueID(n.ID())
		if err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = id

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
