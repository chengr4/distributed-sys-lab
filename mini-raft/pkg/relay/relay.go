package relay
import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"mini-raft/pkg/raft"
	"net"
	"sync"
	"syscall"
	"time"
)

// Action and Filter definitions...

// ServeTCP starts the network service for the relay.
func (r *Relay) ServeTCP(port string) error {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				// Enable address reuse to avoid TIME_WAIT issues
				syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
		},
	}

	listener, err := lc.Listen(context.Background(), "tcp", "0.0.0.0:"+port)
	if err != nil {
		return err
	}

	r.rwmu.Lock()
	r.listener = listener
	r.rwmu.Unlock()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // Listener was closed
			}
			go r.handleConnection(conn)
		}
	}()

	return nil
}

// Stop closes the relay listener.
func (r *Relay) Stop() {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	if r.listener != nil {
		r.listener.Close()
	}
}

func (r *Relay) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		var msg raft.Message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			continue
		}

		addr, action := r.ResolveTarget(&msg)
		if action == Drop {
			continue
		}

		// Forwarding logic
		go r.forward(addr, line)
		if action == Duplicate {
			go r.forward(addr, line)
		}
	}
}

func (r *Relay) forward(addr string, line string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	defer conn.Close()
	fmt.Fprint(conn, line)
}

// Action defines what the relay should do with a message.
type Action int

const (
	Drop Action = iota
	Forward
	Duplicate
)

// Filter defines the interface for intercepting and manipulating message flow.
type Filter interface {
	// Should returns the action to be taken for the given message.
	Should(msg *raft.Message) Action
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

func (d *DelayRule) Should(msg *raft.Message) Action {
	time.Sleep(d.Delay)
	return Forward
}

// DropRule simulates random packet loss based on a probability.
type DropRule struct {
	Probability float64
	rng         *rand.Rand
}

func NewDropRule(probability float64) *DropRule {
	return &DropRule{
		Probability: probability,
		rng:         rand.New(rand.NewPCG(1, 1)),
	}
}

func (d *DropRule) Should(msg *raft.Message) Action {
	if d.rng.Float64() < d.Probability {
		return Drop
	}
	return Forward
}

// PartitionRule splits the network into isolated groups.
type PartitionRule struct {
	Groups map[string]int
}

func NewPartitionRule(groups map[string]int) *PartitionRule {
	return &PartitionRule{
		Groups: groups,
	}
}

func (p *PartitionRule) Should(msg *raft.Message) Action {
	fromGroup, okFrom := p.Groups[msg.From]
	toGroup, okTo := p.Groups[msg.To]

	if !okFrom || !okTo || fromGroup != toGroup {
		return Drop
	}

	return Forward
}

// JitterRule adds random small delays, causing natural message reordering.
type JitterRule struct {
	MaxJitter time.Duration
	rng       *rand.Rand
}

func NewJitterRule(maxJitter time.Duration) *JitterRule {
	return &JitterRule{
		MaxJitter: maxJitter,
		rng:       rand.New(rand.NewPCG(2, 2)),
	}
}

func (j *JitterRule) Should(msg *raft.Message) Action {
	jitter := time.Duration(j.rng.Int64N(int64(j.MaxJitter)))
	time.Sleep(jitter)
	return Forward
}

// DuplicateRule randomly decides to send a message twice.
type DuplicateRule struct {
	Probability float64
	rng         *rand.Rand
}

func NewDuplicateRule(probability float64) *DuplicateRule {
	return &DuplicateRule{
		Probability: probability,
		rng:         rand.New(rand.NewPCG(3, 3)),
	}
}

func (d *DuplicateRule) Should(msg *raft.Message) Action {
	if d.rng.Float64() < d.Probability {
		return Duplicate
	}
	return Forward
}

// Relay acts as a central hub for message routing and failure injection.
type Relay struct {
	rwmu         sync.RWMutex
	routingTable map[string]string // NodeID -> Network Address
	filters      []Filter
	listener     net.Listener
}

// AddFilter registers a new filter to the relay.
func (r *Relay) AddFilter(filter Filter) {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	r.filters = append(r.filters, filter)
}

// ClearFilters removes all registered filters.
func (r *Relay) ClearFilters() {
	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	r.filters = make([]Filter, 0)
}

// ResolveTarget determines the destination address and the action to take.
func (r *Relay) ResolveTarget(msg *raft.Message) (string, Action) {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()

	// 1. Check filters
	finalAction := Forward
	for _, filter := range r.filters {
		action := filter.Should(msg)
		if action == Drop {
			return "", Drop
		}
		if action == Duplicate {
			finalAction = Duplicate
		}
	}

	// 2. Resolve target from routing table
	addr, ok := r.routingTable[msg.To]
	if !ok {
		return "", Drop
	}

	return addr, finalAction
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
