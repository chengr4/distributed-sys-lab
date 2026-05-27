package relay

import (
	"math/rand/v2"
	"mini-raft/pkg/raft"
	"sync"
	"time"
)

// Filter defines the interface for intercepting and potentially dropping messages.
type Filter interface {
	// ShouldForward returns true if the message should be allowed to pass.
	ShouldForward(msg *raft.Message) bool
}

// DelayRule simulates network latency by sleeping for a specific duration.
type DelayRule struct {
	Delay time.Duration
}

func NewDelayRule(delay time.Duration) *DelayRule {
	return &DelayRule{
		Delay: delay,
	}
}

func (d *DelayRule) ShouldForward(msg *raft.Message) bool {
	time.Sleep(d.Delay)
	return true
}

// DropRule simulates random packet loss based on a probability.
type DropRule struct {
	// Probability of dropping a message (0.0 to 1.0).
	Probability float64
	// Seed for random number generation to make it deterministic if needed.
	rng *rand.Rand
}

func NewDropRule(probability float64) *DropRule {
	return &DropRule{
		Probability: probability,
		rng:         rand.New(rand.NewPCG(1, 1)),
	}
}

func (d *DropRule) ShouldForward(msg *raft.Message) bool {
	return d.rng.Float64() >= d.Probability
}

// Relay acts as a central hub for message routing and failure injection.
type Relay struct {
	// rwmu protects routingTable and filters for concurrent access.
	rwmu         sync.RWMutex
	routingTable map[string]string // NodeID -> Network Address
	filters      []Filter
}

// AddFilter registers a new filter to the relay.
func (r *Relay) AddFilter(filter Filter) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	r.filters = append(r.filters, filter)
}

// ResolveTarget determines the destination address for a message after checking filters.
func (r *Relay) ResolveTarget(msg *raft.Message) (string, bool) {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()

	// 1. Check if any filter drops the message
	for _, filter := range r.filters {
		if !filter.ShouldForward(msg) {
			return "", false
		}
	}

	// 2. Resolve target from routing table
	addr, ok := r.routingTable[msg.To]
	return addr, ok
}

// RegisterNode adds or updates a node's address in the routing table.
func (r *Relay) RegisterNode(id string, addr string) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	r.routingTable[id] = addr
}

// NewRelay creates a new instance of Relay.
func NewRelay() *Relay {
	return &Relay{
		routingTable: make(map[string]string),
		filters:      make([]Filter, 0),
	}
}
