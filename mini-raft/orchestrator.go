package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
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
	// 1. Pre-build Phase (Serial to avoid lock contention)
	fmt.Println("\033[1m[1/2] Building components...\033[0m")

	fmt.Print("Building Rust nodes... ")
	rustBuild := exec.Command("cargo", "build")
	rustBuild.Dir = "raft-rust"
	if err := rustBuild.Run(); err != nil {
		fmt.Println("\033[31mFAILED\033[0m")
		log.Fatalf("Rust build error: %v", err)
	}
	fmt.Println("\033[32mDONE\033[0m")

	fmt.Print("Building Relay server... ")
	goBuild := exec.Command("go", "build", "-o", "relay_bin", "cmd/relay/main.go")
	if err := goBuild.Run(); err != nil {
		fmt.Println("\033[31mFAILED\033[0m")
		log.Fatalf("Go build error: %v", err)
	}
	fmt.Println("\033[32mDONE\033[0m")

	// 2. Runtime Phase
	fmt.Println("\n\033[1m[2/2] Launching cluster...\033[0m")
	var processes []ProcessInfo

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Launch Relay
	relayCmd := exec.Command("./relay_bin")
	relayCmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	relayCmd.Stdout = &TaggedWriter{Prefix: "Relay", Color: "\033[31m", Writer: os.Stdout}
	relayCmd.Stderr = relayCmd.Stdout
	if err := relayCmd.Start(); err != nil {
		log.Fatalf("Failed to start Relay: %v", err)
	}
	processes = append(processes, ProcessInfo{ID: "Relay", Cmd: relayCmd})
	fmt.Println("Relay started.")

	time.Sleep(500 * time.Millisecond)

	// Launch Nodes
	nodes := []struct {
		id   string
		port string
	}{
		{"A", "9001"},
		{"B", "9002"},
		{"C", "9003"},
	}
	relayAddr := "127.0.0.1:8080"

	for i, n := range nodes {
		peers := []string{}
		for _, other := range nodes {
			if other.id != n.id {
				peers = append(peers, other.id)
			}
		}

		args := append([]string{n.id, n.port, relayAddr}, peers...)
		cmd := exec.Command("./target/debug/raft-rust", args...)
		cmd.Dir = "raft-rust"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		
		writer := &TaggedWriter{
			Prefix: n.id,
			Color:  colors[i%len(colors)],
			Writer: os.Stdout,
		}
		cmd.Stdout = writer
		cmd.Stderr = writer

		if err := cmd.Start(); err != nil {
			log.Printf("Node %s failed to start: %v", n.id, err)
			continue
		}
		processes = append(processes, ProcessInfo{ID: n.id, Cmd: cmd})
	}

	fmt.Println("\n>>> Cluster running. Press Ctrl+C to stop. <<<")

	<-sigChan
	fmt.Println("\n\n\033[1mShutting down...\033[0m")

	for _, p := range processes {
		fmt.Printf("Cleaning up %s... ", p.ID)
		// Send SIGKILL to the process group
		syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
		p.Cmd.Wait()
		fmt.Println("\033[32mOK\033[0m")
	}

	fmt.Println("\033[1mAll processes terminated. Clean exit.\033[0m")
}
