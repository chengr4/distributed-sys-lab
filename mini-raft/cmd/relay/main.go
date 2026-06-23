package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"syscall"

	"mini-raft/pkg/raft"
	"mini-raft/pkg/relay"
)

func main() {
	relayPort := "8080"
	r := relay.NewRelay()

	// Register known nodes (Prototype)
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	// Use ListenConfig to set SO_REUSEADDR via Control function
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
		},
	}

	listener, err := lc.Listen(context.Background(), "tcp", "0.0.0.0:"+relayPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	fmt.Printf("Relay Server listening on port %s\n", relayPort)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go handleRelayConnection(conn, r)
	}
}

func handleRelayConnection(conn net.Conn, r *relay.Relay) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error: %v", err)
			}
			break
		}

		var msg raft.Message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			log.Printf("Unmarshal error: %v", err)
			continue
		}

		// Resolve target and action
		addr, action := r.ResolveTarget(&msg)
		if action == relay.Drop {
			continue
		}

		// Forward message
		go forwardMessage(addr, line)
		
		// If Duplicate, send it once more
		if action == relay.Duplicate {
			go forwardMessage(addr, line)
		}
	}
}

func forwardMessage(addr string, line string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("Failed to dial %s: %v", addr, err)
		return
	}
	defer conn.Close()
	fmt.Fprint(conn, line)
}
