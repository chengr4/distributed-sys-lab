package relay

import (
	"mini-raft/pkg/raft"
	"testing"
	"time"
)

type MockFilter struct {
	shouldAllow bool
}

func (m *MockFilter) ShouldForward(msg *raft.Message) bool {
	return m.shouldAllow
}

func Test_ResolveTarget(t *testing.T) {
	t.Run("Valid routing without rules", func(t *testing.T) {
		r := NewRelay()

		r.RegisterNode("Node-A", "10.0.0.1:8080")

		msg := &raft.Message{To: "Node-A"}
		addr, ok := r.ResolveTarget(msg)
		if !ok || addr != "10.0.0.1:8080" {
			t.Errorf("Expected addr 10.0.0.1:8080 and true, got %s and %v", addr, ok)
		}
	})

	t.Run("Unknown destination", func(t *testing.T) {
		r := NewRelay()

		msg := &raft.Message{To: "Node-Unknown"}
		_, ok := r.ResolveTarget(msg)
		if ok {
			t.Errorf("Expected false for unknown destination, got true")
		}
	})

	t.Run("Message Dropped by network intercept", func(t *testing.T) {
		r := NewRelay()
		r.RegisterNode("Node-A", "10.0.0.1:8080")

		r.AddFilter(&MockFilter{shouldAllow: false})

		msg := &raft.Message{To: "Node-A"}
		_, ok := r.ResolveTarget(msg)
		if ok {
			t.Errorf("Expected false for message dropped by filter, got true")
		}
	})
}

func Test_DropRule(t *testing.T) {
	msg := &raft.Message{}

	t.Run("Drop all messages when probability is 1.0", func(t *testing.T) {
		rule := NewDropRule(1.0)
		for range 100 {
			if rule.ShouldForward(msg) {
				t.Errorf("Expected message to be dropped at 1.0 probability")
			}
		}
	})

	t.Run("Forward all messages when probability is 0.0", func(t *testing.T) {
		rule := NewDropRule(0.0)
		for range 100 {
			if !rule.ShouldForward(msg) {
				t.Errorf("Expected message to be forwarded at 0.0 probability")
			}
		}
	})

	t.Run("Deterministic behavior with fixed seed", func(t *testing.T) {
		rule := NewDropRule(0.5)
		// With fixed seed NewPCG(1, 1), the first 5 results should be stable.
		var results []bool
		for i := 0; i < 5; i++ {
			results = append(results, rule.ShouldForward(msg))
		}

		// Expected results for PCG(1, 1) at 0.5 probability as observed in test execution.
		expected := []bool{false, true, true, true, false}
		for i, v := range results {
			if v != expected[i] {
				t.Errorf("At step %d, expected %v, got %v", i, expected[i], v)
			}
		}
	})
}

func Test_DelayRule(t *testing.T) {
	msg := &raft.Message{}
	delay := 100 * time.Millisecond

	t.Run("Verify delay duration", func(t *testing.T) {
		rule := NewDelayRule(delay)
		start := time.Now()
		
		if !rule.ShouldForward(msg) {
			t.Errorf("DelayRule should always return true")
		}
		
		elapsed := time.Since(start)
		if elapsed < delay {
			t.Errorf("Expected delay of at least %v, got %v", delay, elapsed)
		}
	})
}
