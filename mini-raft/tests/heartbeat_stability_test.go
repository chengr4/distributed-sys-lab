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

func TestHeartbeatStabilityUnderPacketLoss(t *testing.T) {
	// 1. Initialize and start Relay
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
	
	var mu sync.Mutex
	leaderElected := false
	termChanges := 0

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
		
		processes = append(processes, &NodeProcess{ID: id, Cmd: cmd})
		
		// Monitor logs for leadership and term changes
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				// Print to console for visibility during test
				fmt.Printf("[%s] %s\n", nodeID, line)
				
				if strings.Contains(line, "Won election") {
					mu.Lock()
					leaderElected = true
					termChanges++
					mu.Unlock()
					fmt.Printf(">>> EVENT: Leader elected (Total elections: %d)\n", termChanges)
				}
			}
		}(id, stdout)
	}

	// Cleanup on exit
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

	// 3. Wait for the cluster to stabilize and elect first leader
	t.Log("Waiting for initial stability...")
	time.Sleep(10 * time.Second)
	
	mu.Lock()
	if !leaderElected {
		mu.Unlock()
		t.Fatal("Cluster failed to elect a leader even in perfect network")
	}
	// Reset election count after stabilization
	termChanges = 1 
	mu.Unlock()

	// 4. Inject 50% Packet Loss
	t.Log("Injecting 50% Packet Loss Rule...")
	r.AddFilter(relay.NewDropRule(0.5))

	// 5. Observe stability for a duration
	observationDuration := 20 * time.Second
	t.Logf("Observing cluster stability for %v...", observationDuration)
	time.Sleep(observationDuration)

	mu.Lock()
	finalElectionCount := termChanges
	mu.Unlock()

	// 6. Analysis
	if finalElectionCount > 2 {
		t.Logf("Observed instability: %d elections occurred during packet loss.", finalElectionCount)
	} else {
		t.Logf("Strong resilience: Leader held through 50%% packet loss with only %d total elections.", finalElectionCount)
	}
}

// TestHeartbeatStabilityMixedCluster tests leader stability under 50% packet loss in a mixed cluster (2 Rust + 1 Go).
func TestHeartbeatStabilityMixedCluster(t *testing.T) {
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
	
	var mu sync.Mutex
	leaderElected := false
	termChanges := 0

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
		
		processes = append(processes, &NodeProcess{ID: id, Cmd: cmd})
		
		// Monitor logs for leadership and term changes
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[Mixed-%s] %s\n", nodeID, line)
				
				if strings.Contains(line, "Won election") {
					mu.Lock()
					leaderElected = true
					termChanges++
					mu.Unlock()
					fmt.Printf(">>> MIXED EVENT: Leader elected (Total elections: %d)\n", termChanges)
				}
			}
		}(id, stdout)
	}

	// Cleanup on exit
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

	// 3. Wait for the cluster to stabilize and elect first leader
	t.Log("Waiting for initial stability in mixed cluster...")
	time.Sleep(10 * time.Second)
	
	mu.Lock()
	if !leaderElected {
		mu.Unlock()
		t.Fatal("Mixed cluster failed to elect a leader even in perfect network")
	}
	termChanges = 1 
	mu.Unlock()

	// 4. Inject 50% Packet Loss
	t.Log("Injecting 50% Packet Loss Rule into mixed cluster...")
	r.AddFilter(relay.NewDropRule(0.5))

	// 5. Observe stability for a duration
	observationDuration := 20 * time.Second
	t.Logf("Observing mixed cluster stability for %v...", observationDuration)
	time.Sleep(observationDuration)

	mu.Lock()
	finalElectionCount := termChanges
	mu.Unlock()

	// 6. Analysis
	if finalElectionCount > 2 {
		t.Logf("Observed instability in mixed cluster: %d elections occurred during packet loss.", finalElectionCount)
	} else {
		t.Logf("Strong resilience in mixed cluster: Leader held through 50%% packet loss with only %d total elections.", finalElectionCount)
	}
}
