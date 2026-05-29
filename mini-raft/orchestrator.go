package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ProcessInfo struct {
	ID  string
	Cmd *exec.Cmd
}

// TaggedWriter prepends a colored prefix to each line of output.
type TaggedWriter struct {
	Prefix string
	Color  string
	Writer io.Writer
	buf    bytes.Buffer
}

func (tw *TaggedWriter) Write(p []byte) (n int, err error) {
	for _, b := range p {
		tw.buf.WriteByte(b)
		if b == '\n' {
			line := tw.buf.String()
			tw.buf.Reset()
			// Format: [Color][Prefix][Reset] Message
			fmt.Fprintf(tw.Writer, "%s[%s]%s %s", tw.Color, tw.Prefix, "\033[0m", line)
		}
	}
	return len(p), nil
}

var colors = []string{
	"\033[32m", // Green
	"\033[33m", // Yellow
	"\033[34m", // Blue
	"\033[35m", // Magenta
	"\033[36m", // Cyan
}

func main() {
	// 1. Build Rust project
	fmt.Println("Building Rust node...")
	buildCmd := exec.Command("cargo", "build")
	buildCmd.Dir = "raft-rust"
	buildCmd.Stdout = os.Stdout
	buildCmd.Stderr = os.Stderr
	if err := buildCmd.Run(); err != nil {
		log.Fatalf("Failed to build Rust: %v", err)
	}

	// Track all processes for cleanup
	var processes []ProcessInfo
	var mu sync.Mutex

	// Signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 2. Start Relay
	fmt.Println("Starting Relay Server...")
	relayCmd := exec.Command("go", "run", "cmd/relay/main.go")
	relayCmd.Stdout = &TaggedWriter{Prefix: "Relay", Color: "\033[31m", Writer: os.Stdout}
	relayCmd.Stderr = relayCmd.Stdout
	if err := relayCmd.Start(); err != nil {
		log.Fatalf("Failed to start Relay: %v", err)
	}
	mu.Lock()
	processes = append(processes, ProcessInfo{ID: "Relay", Cmd: relayCmd})
	mu.Unlock()

	time.Sleep(1 * time.Second) // Wait for relay to start

	// 3. Start Nodes
	nodes := []struct {
		id    string
		port  string
		peers []string
	}{
		{"A", "9001", []string{"B", "C"}},
		{"B", "9002", []string{"A", "C"}},
		{"C", "9003", []string{"A", "B"}},
	}

	relayAddr := "127.0.0.1:8080"
	var wg sync.WaitGroup

	for i, n := range nodes {
		wg.Add(1)
		go func(i int, id, port string, peers []string) {
			defer wg.Done()

			// Use 'cargo run' to execute the binary
			args := append([]string{"run", "--bin", "raft-rust", "--", id, port, relayAddr}, peers...)
			cmd := exec.Command("cargo", args...)
			cmd.Dir = "raft-rust"

			// Use TaggedWriter for colored output
			writer := &TaggedWriter{
				Prefix: id,
				Color:  colors[i%len(colors)],
				Writer: os.Stdout,
			}
			cmd.Stdout = writer
			cmd.Stderr = writer

			if err := cmd.Start(); err != nil {
				log.Printf("Node %s failed to start: %v", id, err)
				return
			}

			mu.Lock()
			processes = append(processes, ProcessInfo{ID: id, Cmd: cmd})
			mu.Unlock()

			// Wait for the command to finish (or be killed)
			if err := cmd.Wait(); err != nil {
				// Avoid logging error if it was killed by orchestrator
				if exitErr, ok := err.(*exec.ExitError); ok {
					if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
						return
					}
				}
				log.Printf("Node %s exited with error: %v", id, err)
			}
		}(i, n.id, n.port, n.peers)
	}

	fmt.Println("\n>>> Cluster started. Press Ctrl+C to stop and cleanup. <<<")

	// Wait for Ctrl+C
	<-sigChan
	fmt.Println("\nShutting down and cleaning up processes...")

	mu.Lock()
	for _, p := range processes {
		fmt.Printf("Killing process: %s (PID: %d)...\n", p.ID, p.Cmd.Process.Pid)
		if err := p.Cmd.Process.Kill(); err != nil {
			fmt.Printf("Failed to kill %s: %v\n", p.ID, err)
		}
	}
	mu.Unlock()

	fmt.Println("Cleanup complete. Exiting.")
	os.Exit(0)
}
