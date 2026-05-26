use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate {
        votes_received: HashSet<String>,
    },
    Leader {
        // Index of next log entry to send to each peer
        next_indices: HashMap<String, u64>,
        // Index of highest log entry known to be replicated on each peer
        match_indices: HashMap<String, u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub struct Message {
    pub from: String,
    pub to: String,
    pub r#type: String, // 'type' is a reserved keyword in Rust, use r# prefix
    pub payload: Vec<u8>,
}

// The dicision of RaftNode (brain); Relay and engine (body) will execute the side effects
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SideEffect {
    // to the engine
    ResetElectionTimer,
    BroadcastRequestVote(RequestVoteArgs),
    BroadcastAppendEntries(AppendEntriesArgs),
    ApplyEntry { index: u64, command: String },
}
