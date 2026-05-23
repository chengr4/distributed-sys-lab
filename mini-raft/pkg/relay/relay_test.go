package relay

import (
	"mini-raft/pkg/raft"
	"testing"
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
