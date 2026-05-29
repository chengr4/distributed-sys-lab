package relay

import (
	"mini-raft/pkg/raft"
	"testing"
	"time"
)

// MockFilter 用於測試，模擬 Filter 介面
type MockFilter struct {
	action Action
}

func (m *MockFilter) Should(msg *raft.Message) Action {
	return m.action
}

func Test_ResolveTarget(t *testing.T) {
	t.Run("Valid routing without rules", func(t *testing.T) {
		r := NewRelay()
		r.RegisterNode("Node-A", "10.0.0.1:8080")

		msg := &raft.Message{To: "Node-A"}
		addr, action := r.ResolveTarget(msg)
		if action != Forward || addr != "10.0.0.1:8080" {
			t.Errorf("Expected addr 10.0.0.1:8080 and Forward, got %s and %v", addr, action)
		}
	})

	t.Run("Message Dropped by filter", func(t *testing.T) {
		r := NewRelay()
		r.RegisterNode("Node-A", "10.0.0.1:8080")
		r.AddFilter(&MockFilter{action: Drop})

		msg := &raft.Message{To: "Node-A"}
		_, action := r.ResolveTarget(msg)
		if action != Drop {
			t.Errorf("Expected Drop, got %v", action)
		}
	})

	t.Run("Message Duplicated by filter", func(t *testing.T) {
		r := NewRelay()
		r.RegisterNode("Node-A", "10.0.0.1:8080")
		r.AddFilter(&MockFilter{action: Duplicate})

		msg := &raft.Message{To: "Node-A"}
		_, action := r.ResolveTarget(msg)
		if action != Duplicate {
			t.Errorf("Expected Duplicate, got %v", action)
		}
	})
}

func Test_DropRule(t *testing.T) {
	msg := &raft.Message{}
	t.Run("Drop all messages at 1.0 probability", func(t *testing.T) {
		rule := NewDropRule(1.0)
		if rule.Should(msg) != Drop {
			t.Errorf("Expected Drop")
		}
	})
}

func Test_DuplicateRule(t *testing.T) {
	msg := &raft.Message{}
	t.Run("Duplicate all messages at 1.0 probability", func(t *testing.T) {
		rule := NewDuplicateRule(1.0)
		if rule.Should(msg) != Duplicate {
			t.Errorf("Expected Duplicate")
		}
	})
}

func Test_JitterRule(t *testing.T) {
	msg := &raft.Message{}
	t.Run("Jitter should forward", func(t *testing.T) {
		rule := NewJitterRule(10 * time.Millisecond)
		if rule.Should(msg) != Forward {
			t.Errorf("Expected Forward")
		}
	})
}

func Test_DelayRule(t *testing.T) {
	msg := &raft.Message{}
	t.Run("Delay should forward", func(t *testing.T) {
		rule := NewDelayRule(10 * time.Millisecond)
		if rule.Should(msg) != Forward {
			t.Errorf("Expected Forward")
		}
	})
}

func Test_PartitionRule(t *testing.T) {
	groups := map[string]int{"A": 1, "B": 2}
	rule := NewPartitionRule(groups)
	t.Run("Different groups should Drop", func(t *testing.T) {
		msg := &raft.Message{From: "A", To: "B"}
		if rule.Should(msg) != Drop {
			t.Errorf("Expected Drop")
		}
	})
}
