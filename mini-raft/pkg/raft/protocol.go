package raft

// LogEntry represents a single log entry.
type LogEntry struct {
	Term    uint64 `json:"term"`
	Index   uint64 `json:"index"`
	Command string `json:"command"`
}

// RequestVoteArgs is the argument for requesting a vote.
type RequestVoteArgs struct {
	Term         uint64 `json:"term"` // Current term of the candidate
	CandidateID  string `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteReply is the response to a vote request.
type RequestVoteReply struct {
	Term        uint64 `json:"term"` // Current term of the responder
	VoteGranted bool   `json:"vote_granted"`
}

// AppendEntriesArgs is used for both log replication and heartbeats.
// Term, PrevLogIndex, and PrevLogTerm ensure the Idempotency; they ensure that each term can only have one leader and logs are always matched.
type AppendEntriesArgs struct {
	Term         uint64     `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint64     `json:"leader_commit"` // Index of the highest log entry known to be committed by the leader. Used by followers to update their commit index.
}

// AppendEntriesReply is the response to an append entries request.
type AppendEntriesReply struct {
	Term    uint64 `json:"term"` // Current term of the responder
	Success bool   `json:"success"`
}

// Message is the generic envelope for all node-to-node communication.
// From and To allow Relay to know the route without needing to inspect the payload
// Payload implements the decoupling of the transport layer and the logic layer
type Message struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Type    string `json:"type"`    // "RequestVote", "AppendEntries", "VoteReply", "AppendReply"
	Payload []byte `json:"payload"` //
}
