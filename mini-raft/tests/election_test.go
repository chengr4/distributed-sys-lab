package tests

import (
	"bufio"
	"fmt"
	"io"
	"mini-raft/pkg/relay"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestLeaderElectionIsolation tests basic election under partition.
func TestLeaderElectionIsolation(t *testing.T) {
	// 1. Initialize and start Relay (embedded in test)
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")

	if err := r.ServeTCP("8080"); err != nil {
		t.Fatalf("Failed to start relay: %v", err)
	}

	// 2. Start nodes
	nodes := []string{"A", "B", "C"}
	processes := make([]*NodeProcess, 0)

	// Used to track the current Leader
	var mu sync.Mutex
	currentLeader := ""

	for _, id := range nodes {
		binPath, binDir := getBinPath()
		args := append([]string{id, getPort(id), "127.0.0.1:8080"}, getPeers(id, nodes)...)
		cmd := exec.Command(binPath, args...)
		cmd.Dir = binDir
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", id, err)
		}

		p := &NodeProcess{ID: id, Cmd: cmd}
		processes = append(processes, p)

		// Asynchronously read logs and monitor Leader status
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[%s] %s\n", nodeID, line)

				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
					fmt.Printf(">>> TEST OBSERVED: Node %s became leader\n", nodeID)
				}
			}
		}(id, stdout)
	}

	// Ensure processes are cleaned up on exit
	defer func() {
		r.Stop()
		var wg sync.WaitGroup
		for _, p := range processes {
			_ = syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
			wg.Add(1)
			go func(proc *NodeProcess) {
				defer wg.Done()
				_ = proc.Cmd.Wait()
			}(p)
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Log("Warning: cleanup timed out")
		}
	}()

	// 3. Wait for the first Leader to be elected
	t.Log("Waiting for initial leader...")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		if currentLeader != "" {
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}

	mu.Lock()
	if currentLeader == "" {
		mu.Unlock()
		t.Fatal("No leader elected within deadline")
	}
	leader := currentLeader
	mu.Unlock()

	// 4. Inject Partition: Isolate the current Leader
	t.Logf("Injecting Partition: Isolating Leader %s...", leader)
	groups := map[string]int{"A": 1, "B": 1, "C": 1}
	groups[leader] = 2 // Move Leader to a different group

	r.AddFilter(relay.NewPartitionRule(groups))

	// 5. Wait for remaining nodes to elect a new Leader
	t.Log("Waiting for new leader selection in minority...")
	time.Sleep(10 * time.Second)

	// 6. Restore network, observe old Leader step down
	t.Log("Restoring network...")
	r.ClearFilters()

	time.Sleep(5 * time.Second)
}

// TestLeaderElectionMixedCluster tests election stability and recovery in a mixed cluster (Go and Rust nodes).
func TestLeaderElectionMixedCluster(t *testing.T) {
	// 1. Initialize and start Relay
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")

	if err := r.ServeTCP("8080"); err != nil {
		t.Fatalf("Failed to start relay: %v", err)
	}

	// 2. Start nodes with mixed implementations:
	// A: Rust, B: Rust, C: Go
	nodes := []string{"A", "B", "C"}
	nodeImpls := map[string]string{
		"A": "rust",
		"B": "rust",
		"C": "go",
	}
	processes := make([]*NodeProcess, 0)

	// Used to track the current Leader
	var mu sync.Mutex
	currentLeader := ""

	for _, id := range nodes {
		impl := nodeImpls[id]
		binPath, binDir := getBinPathForImpl(impl)
		
		t.Logf("Starting Node %s with [%s] implementation...", id, impl)
		args := append([]string{id, getPort(id), "127.0.0.1:8080"}, getPeers(id, nodes)...)
		cmd := exec.Command(binPath, args...)
		cmd.Dir = binDir
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %s [%s]: %v", id, impl, err)
		}

		p := &NodeProcess{ID: id, Cmd: cmd}
		processes = append(processes, p)

		// Asynchronously read logs and monitor Leader status
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[Mixed-%s] %s\n", nodeID, line)

				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
					fmt.Printf(">>> MIXED TEST OBSERVED: Node %s became leader\n", nodeID)
				}
			}
		}(id, stdout)
	}

	// Ensure processes are cleaned up on exit
	defer func() {
		r.Stop()
		var wg sync.WaitGroup
		for _, p := range processes {
			_ = syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
			wg.Add(1)
			go func(proc *NodeProcess) {
				defer wg.Done()
				_ = proc.Cmd.Wait()
			}(p)
		}
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Log("Warning: cleanup timed out")
		}
	}()

	// 3. Wait for the first Leader to be elected
	t.Log("Waiting for initial leader in mixed cluster...")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		if currentLeader != "" {
			mu.Unlock()
			break
		}
		mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}

	mu.Lock()
	if currentLeader == "" {
		mu.Unlock()
		t.Fatal("No leader elected within deadline in mixed cluster")
	}
	leader := currentLeader
	mu.Unlock()

	// 4. Inject Partition: Isolate the current Leader
	t.Logf("Injecting Partition: Isolating Leader %s from mixed cluster...", leader)
	groups := map[string]int{"A": 1, "B": 1, "C": 1}
	groups[leader] = 2 // Move Leader to a different group

	r.AddFilter(relay.NewPartitionRule(groups))

	// 5. Wait for remaining nodes to elect a new Leader
	t.Log("Waiting for new leader selection in minority of mixed cluster...")
	time.Sleep(10 * time.Second)

	// 6. Restore network, observe old Leader step down
	t.Log("Restoring network in mixed cluster...")
	r.ClearFilters()

	time.Sleep(5 * time.Second)
}

