package tests

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"mini-raft/pkg/raft"
	"mini-raft/pkg/relay"
	"net"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestLogReplicationBasic(t *testing.T) {
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	// 1. Start Relay in goroutine (Non-blocking)
	go r.ServeTCP("8080")
	defer r.Stop()

	nodes := []string{"A", "B", "C"}
	processes := make([]*NodeProcess, 0)
	var mu sync.Mutex
	
	// 2. Register cleanup defer EARLY
	defer func() {
		for _, p := range processes {
			if p.Cmd != nil && p.Cmd.Process != nil {
				syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
				p.Cmd.Wait()
			}
		}
	}()

	currentLeader := ""
	appliedCount := make(map[string]int)

	for _, id := range nodes {
		args := append([]string{id, getPort(id), "127.0.0.1:8080"}, getPeers(id, nodes)...)
		cmd := exec.Command("../raft-rust/target/debug/raft-rust", args...)
		cmd.Dir = "../raft-rust"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", id, err)
		}
		
		p := &NodeProcess{ID: id, Cmd: cmd}
		processes = append(processes, p)
		
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[%s] %s\n", nodeID, line)
				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
				}
				if strings.Contains(line, "*** STATE MACHINE APPLY ***") {
					mu.Lock()
					appliedCount[nodeID]++
					mu.Unlock()
				}
			}
		}(id, stdout)
	}

	t.Log("Waiting for leader election...")
	if err := waitLeader(&mu, &currentLeader, time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	leader := currentLeader
	mu.Unlock()

	clientListener, _ := net.Listen("tcp", "127.0.0.1:9999")
	defer clientListener.Close()

	t.Logf("Sending ClientRequest to Leader %s...", leader)
	reqArgs := raft.ClientRequestArgs{Command: "BasicTest-1"}
	payload, _ := json.Marshal(reqArgs)
	msg := raft.Message{From: "Client", To: leader, Type: "ClientRequest", Payload: payload}

	conn, _ := net.Dial("tcp", "127.0.0.1:8080")
	json.NewEncoder(conn).Encode(msg)
	conn.Close()

	if l, ok := clientListener.(*net.TCPListener); ok {
		l.SetDeadline(time.Now().Add(5 * time.Second))
	}
	replyConn, err := clientListener.Accept()
	if err != nil {
		t.Fatalf("Failed to receive reply: %v", err)
	}
	replyConn.Close()

	t.Log("Waiting for replication...")
	time.Sleep(5 * time.Second)

	mu.Lock()
	totalApplied := 0
	for _, c := range appliedCount {
		if c >= 1 { totalApplied++ }
	}
	mu.Unlock()

	if totalApplied < 2 {
		t.Fatalf("Replication failed: only %d nodes applied", totalApplied)
	}
	t.Logf("SUCCESS: %d nodes applied command.", totalApplied)
}

func TestLogReplicationWithPartition(t *testing.T) {
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	go r.ServeTCP("8080")
	defer r.Stop()

	nodes := []string{"A", "B", "C"}
	processes := make([]*NodeProcess, 0)
	var mu sync.Mutex
	
	defer func() {
		for _, p := range processes {
			if p.Cmd != nil && p.Cmd.Process != nil {
				syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
				p.Cmd.Wait()
			}
		}
	}()

	currentLeader := ""
	appliedCount := make(map[string]int)

	for _, id := range nodes {
		args := append([]string{id, getPort(id), "127.0.0.1:8080"}, getPeers(id, nodes)...)
		cmd := exec.Command("../raft-rust/target/debug/raft-rust", args...)
		cmd.Dir = "../raft-rust"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", id, err)
		}
		
		p := &NodeProcess{ID: id, Cmd: cmd}
		processes = append(processes, p)
		
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[%s] %s\n", nodeID, line)
				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
				}
				if strings.Contains(line, "*** STATE MACHINE APPLY ***") {
					mu.Lock()
					appliedCount[nodeID]++
					mu.Unlock()
				}
			}
		}(id, stdout)
	}

	t.Log("Waiting for stable leader...")
	if err := waitLeader(&mu, &currentLeader, time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	
	mu.Lock()
	leader := currentLeader
	mu.Unlock()

	isolatedFollower := "A"
	if leader == "A" { isolatedFollower = "B" }
	t.Logf("Isolating %s...", isolatedFollower)
	groups := map[string]int{"A": 1, "B": 1, "C": 1, "Client": 1}
	groups[isolatedFollower] = 2
	r.AddFilter(relay.NewPartitionRule(groups))
	time.Sleep(1 * time.Second)

	t.Logf("Sending command to Leader %s (%s is isolated)...", leader, isolatedFollower)
	reqArgs := raft.ClientRequestArgs{Command: "Partition-Test-Msg"}
	payload, _ := json.Marshal(reqArgs)
	msg := raft.Message{From: "Client", To: leader, Type: "ClientRequest", Payload: payload}

	conn, _ := net.Dial("tcp", "127.0.0.1:8080")
	json.NewEncoder(conn).Encode(msg)
	conn.Close()

	t.Log("Waiting for Leader and available majority to apply...")
	time.Sleep(10 * time.Second)

	mu.Lock()
	appliedOnLeader := appliedCount[leader]
	appliedOnIsolated := appliedCount[isolatedFollower]
	mu.Unlock()

	if appliedOnLeader < 1 {
		t.Errorf("Leader %s failed to apply command via majority quorum.", leader)
	}
	if appliedOnIsolated > 0 {
		t.Errorf("Isolated node %s incorrectly applied command.", isolatedFollower)
	}

	t.Logf("Restoring network. %s should catch up now.", isolatedFollower)
	r.ClearFilters()

	time.Sleep(15 * time.Second) 
	mu.Lock()
	finalAppliedOnIsolated := appliedCount[isolatedFollower]
	mu.Unlock()

	if finalAppliedOnIsolated < 1 {
		t.Fatalf("Follower %s failed to catch up after rejoining cluster", isolatedFollower)
	}
	t.Logf("SUCCESS: Follower %s caught up and applied command.", isolatedFollower)
}

func TestLogReplicationWithPartitionClientRequestDrop(t *testing.T) {
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	go r.ServeTCP("8080")
	defer r.Stop()

	nodes := []string{"A", "B", "C"}
	processes := make([]*NodeProcess, 0)
	var mu sync.Mutex
	
	defer func() {
		for _, p := range processes {
			if p.Cmd != nil && p.Cmd.Process != nil {
				syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
				p.Cmd.Wait()
			}
		}
	}()

	currentLeader := ""
	appliedCount := make(map[string]int)

	for _, id := range nodes {
		args := append([]string{id, getPort(id), "127.0.0.1:8080"}, getPeers(id, nodes)...)
		cmd := exec.Command("../raft-rust/target/debug/raft-rust", args...)
		cmd.Dir = "../raft-rust"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", id, err)
		}
		
		p := &NodeProcess{ID: id, Cmd: cmd}
		processes = append(processes, p)
		
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[%s] %s\n", nodeID, line)
				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
				}
				if strings.Contains(line, "*** STATE MACHINE APPLY ***") {
					mu.Lock()
					appliedCount[nodeID]++
					mu.Unlock()
				}
			}
		}(id, stdout)
	}

	t.Log("Waiting for stable leader...")
	if err := waitLeader(&mu, &currentLeader, time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	
	mu.Lock()
	leader := currentLeader
	mu.Unlock()

	isolatedFollower := "C"
	if leader == "C" { isolatedFollower = "B" }
	t.Logf("Isolating %s and excluding 'Client'...", isolatedFollower)
	groups := map[string]int{"A": 1, "B": 1, "C": 1} 
	groups[isolatedFollower] = 2 
	r.AddFilter(relay.NewPartitionRule(groups))
	time.Sleep(1 * time.Second)

	t.Logf("Sending command to Leader %s (Relay should drop this)...", leader)
	reqArgs := raft.ClientRequestArgs{Command: "Dropped-Msg"}
	payload, _ := json.Marshal(reqArgs)
	msg := raft.Message{From: "Client", To: leader, Type: "ClientRequest", Payload: payload}
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err == nil {
		json.NewEncoder(conn).Encode(msg)
		conn.Close()
	}

	t.Log("Verifying command was NOT applied...")
	time.Sleep(5 * time.Second) 
	mu.Lock()
	anyApplied := false
	for _, count := range appliedCount {
		if count > 0 { anyApplied = true }
	}
	mu.Unlock()

	if anyApplied {
		t.Errorf("FAIL: Command was unexpectedly applied.")
	} else {
		t.Log("SUCCESS: Client message was correctly dropped.")
	}
}

func TestLogReplicationPassiveSynchronization(t *testing.T) {
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	go r.ServeTCP("8080")
	defer r.Stop()

	nodes := []string{"A", "B", "C"}
	processes := make([]*NodeProcess, 0)
	var mu sync.Mutex
	
	defer func() {
		for _, p := range processes {
			if p.Cmd != nil && p.Cmd.Process != nil {
				syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
				p.Cmd.Wait()
			}
		}
	}()

	currentLeader := ""
	appliedCount := make(map[string]int)

	for _, id := range nodes {
		args := append([]string{id, getPort(id), "127.0.0.1:8080"}, getPeers(id, nodes)...)
		cmd := exec.Command("../raft-rust/target/debug/raft-rust", args...)
		cmd.Dir = "../raft-rust"
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		
		stdout, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start node %s: %v", id, err)
		}
		
		p := &NodeProcess{ID: id, Cmd: cmd}
		processes = append(processes, p)
		
		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[%s] %s\n", nodeID, line)
				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
				}
				if strings.Contains(line, "*** STATE MACHINE APPLY ***") {
					mu.Lock()
					appliedCount[nodeID]++
					mu.Unlock()
				}
			}
		}(id, stdout)
	}

	t.Log("Waiting for stable leader...")
	if err := waitLeader(&mu, &currentLeader, time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	
	mu.Lock()
	leader := currentLeader
	mu.Unlock()

	isolatedFollower := "C"
	if leader == "C" { isolatedFollower = "B" }
	t.Logf("Isolating %s...", isolatedFollower)
	groups := map[string]int{"A": 1, "B": 1, "C": 1, "Client": 1}
	groups[isolatedFollower] = 2
	r.AddFilter(relay.NewPartitionRule(groups))
	time.Sleep(1 * time.Second)

	t.Logf("Proposing command to Leader %s...", leader)
	reqArgs := raft.ClientRequestArgs{Command: "Passive-Sync-Evidence"}
	payload, _ := json.Marshal(reqArgs)
	msg := raft.Message{From: "Client", To: leader, Type: "ClientRequest", Payload: payload}
	conn, _ := net.Dial("tcp", "127.0.0.1:8080")
	json.NewEncoder(conn).Encode(msg)
	conn.Close()
	
	time.Sleep(2 * time.Second) 

	t.Logf("Restoring %s. OBSERVE LOGS FOR GAP.", isolatedFollower)
	r.ClearFilters()

	t.Log("Waiting 200ms for synchronization (TIGHT DEADLINE)...")
	time.Sleep(200 * time.Millisecond) 

	mu.Lock()
	finalAppliedOnIsolated := appliedCount[isolatedFollower]
	mu.Unlock()

	if finalAppliedOnIsolated < 1 {
		t.Fatalf("CRITICAL: Follower %s failed to catch up within 200ms.", isolatedFollower)
	}
	t.Logf("SUCCESS: Follower %s caught up proactively!", isolatedFollower)
}

// TestLogReplicationBasicMixedCluster tests basic log replication in a mixed cluster (2 Rust + 1 Go).
func TestLogReplicationBasicMixedCluster(t *testing.T) {
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	if err := r.ServeTCP("8080"); err != nil {
		t.Fatalf("Failed to start relay: %v", err)
	}
	defer r.Stop()

	nodes := []string{"A", "B", "C"}
	nodeImpls := map[string]string{
		"A": "rust",
		"B": "rust",
		"C": "go",
	}
	processes := make([]*NodeProcess, 0)

	var mu sync.Mutex
	currentLeader := ""
	appliedCount := make(map[string]int)

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

		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[Mixed-%s] %s\n", nodeID, line)

				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
				}
				if strings.Contains(line, "*** STATE MACHINE APPLY ***") {
					mu.Lock()
					appliedCount[nodeID]++
					mu.Unlock()
				}
			}
		}(id, stdout)
	}

	defer func() {
		for _, p := range processes {
			syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
			p.Cmd.Wait()
		}
	}()

	t.Log("Waiting for leader election in mixed cluster...")
	if err := waitLeader(&mu, &currentLeader, time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	leader := currentLeader
	mu.Unlock()

	clientListener, _ := net.Listen("tcp", "127.0.0.1:9999")
	defer clientListener.Close()

	t.Logf("Sending ClientRequest to Leader %s in mixed cluster...", leader)
	reqArgs := raft.ClientRequestArgs{Command: "BasicTest-Mixed"}
	payload, _ := json.Marshal(reqArgs)
	msg := raft.Message{From: "Client", To: leader, Type: "ClientRequest", Payload: payload}

	conn, _ := net.Dial("tcp", "127.0.0.1:8080")
	json.NewEncoder(conn).Encode(msg)
	conn.Close()

	if l, ok := clientListener.(*net.TCPListener); ok {
		l.SetDeadline(time.Now().Add(5 * time.Second))
	}
	replyConn, err := clientListener.Accept()
	if err != nil {
		t.Fatalf("Failed to receive reply in mixed cluster: %v", err)
	}
	replyConn.Close()

	t.Log("Waiting for replication in mixed cluster...")
	time.Sleep(5 * time.Second)

	mu.Lock()
	totalApplied := 0
	for _, c := range appliedCount {
		if c >= 1 {
			totalApplied++
		}
	}
	mu.Unlock()

	if totalApplied < 2 {
		t.Fatalf("Replication failed in mixed cluster: only %d nodes applied", totalApplied)
	}
	t.Logf("SUCCESS: %d nodes applied command in mixed cluster.", totalApplied)
}

// TestLogReplicationWithPartitionMixedCluster tests replication stability and catchup under partition in a mixed cluster (2 Rust + 1 Go).
func TestLogReplicationWithPartitionMixedCluster(t *testing.T) {
	r := relay.NewRelay()
	r.RegisterNode("A", "127.0.0.1:9001")
	r.RegisterNode("B", "127.0.0.1:9002")
	r.RegisterNode("C", "127.0.0.1:9003")
	r.RegisterNode("Client", "127.0.0.1:9999")

	if err := r.ServeTCP("8080"); err != nil {
		t.Fatalf("Failed to start relay: %v", err)
	}
	defer r.Stop()

	nodes := []string{"A", "B", "C"}
	nodeImpls := map[string]string{
		"A": "rust",
		"B": "rust",
		"C": "go",
	}
	processes := make([]*NodeProcess, 0)

	var mu sync.Mutex
	currentLeader := ""
	appliedCount := make(map[string]int)

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

		go func(nodeID string, reader io.ReadCloser) {
			scanner := bufio.NewScanner(reader)
			for scanner.Scan() {
				line := scanner.Text()
				fmt.Printf("[Mixed-%s] %s\n", nodeID, line)

				if strings.Contains(line, "Won election") {
					mu.Lock()
					currentLeader = nodeID
					mu.Unlock()
				}
				if strings.Contains(line, "*** STATE MACHINE APPLY ***") {
					mu.Lock()
					appliedCount[nodeID]++
					mu.Unlock()
				}
			}
		}(id, stdout)
	}

	defer func() {
		for _, p := range processes {
			syscall.Kill(-p.Cmd.Process.Pid, syscall.SIGKILL)
			p.Cmd.Wait()
		}
	}()

	t.Log("Waiting for stable leader in mixed cluster...")
	if err := waitLeader(&mu, &currentLeader, time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)

	mu.Lock()
	leader := currentLeader
	mu.Unlock()

	isolatedFollower := "A"
	if leader == "A" {
		isolatedFollower = "B"
	}
	t.Logf("Isolating %s in mixed cluster...", isolatedFollower)
	groups := map[string]int{"A": 1, "B": 1, "C": 1, "Client": 1}
	groups[isolatedFollower] = 2
	r.AddFilter(relay.NewPartitionRule(groups))
	time.Sleep(1 * time.Second)

	t.Logf("Sending command to Leader %s (%s is isolated) in mixed cluster...", leader, isolatedFollower)
	reqArgs := raft.ClientRequestArgs{Command: "Partition-Test-Msg-Mixed"}
	payload, _ := json.Marshal(reqArgs)
	msg := raft.Message{From: "Client", To: leader, Type: "ClientRequest", Payload: payload}

	conn, _ := net.Dial("tcp", "127.0.0.1:8080")
	json.NewEncoder(conn).Encode(msg)
	conn.Close()

	t.Log("Waiting for Leader and available majority to apply in mixed cluster...")
	time.Sleep(10 * time.Second)

	mu.Lock()
	appliedOnLeader := appliedCount[leader]
	appliedOnIsolated := appliedCount[isolatedFollower]
	mu.Unlock()

	if appliedOnLeader < 1 {
		t.Errorf("Leader %s failed to apply command via majority quorum in mixed cluster.", leader)
	}
	if appliedOnIsolated > 0 {
		t.Errorf("Isolated node %s incorrectly applied command in mixed cluster.", isolatedFollower)
	}

	t.Logf("Restoring network in mixed cluster. %s should catch up now.", isolatedFollower)
	r.ClearFilters()

	time.Sleep(15 * time.Second)
	mu.Lock()
	finalAppliedOnIsolated := appliedCount[isolatedFollower]
	mu.Unlock()

	if finalAppliedOnIsolated < 1 {
		t.Fatalf("Follower %s failed to catch up after rejoining mixed cluster", isolatedFollower)
	}
	t.Logf("SUCCESS: Follower %s caught up and applied command in mixed cluster.", isolatedFollower)
}
