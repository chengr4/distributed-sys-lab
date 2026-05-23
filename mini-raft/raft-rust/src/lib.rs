use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    Candidate,
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

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

pub struct RaftNode {
    pub id: String,
    pub peers: Vec<String>,
    pub current_term: u64,
    pub voted_for: Option<String>, // each term can only vote for one candidate
    pub log: Vec<LogEntry>,
    pub state: NodeState,

    // Volatile state
    pub committed_index: u64,
    pub last_applied: u64,
}

impl RaftNode {
    pub fn new(id: String, peers: Vec<String>) -> Self {
        let mut log = Vec::new();
        log.push(LogEntry {
            term: 0,
            index: 0,
            command: "sentinel".to_string(),
        });
        Self {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            log,
            state: NodeState::Follower,
            committed_index: 0,
            last_applied: 0,
        }
    }

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // reject lagacy leader (Paper 5.1)
        if args.term < self.current_term {
            return self.reject_append_entries();
        }

        // (Paper 5.2)
        self.apply_leader_term_and_step_down(args.term);

        if !self.has_matching_prev_entry(args.prev_log_index, args.prev_log_term) {
            return self.reject_append_entries();
        }

        // Add new entries and handle conflicts (Paper 5.3 Step 3 & 4)
        let mut last_new_entry_index = args.prev_log_index;
        for entry in args.entries {
            last_new_entry_index = entry.index;
            match self.log.get(entry.index as usize) {
                Some(existing) if existing.term != entry.term => {
                    // Conflict detected, delete the existing entry and all that follow it
                    self.log.truncate(entry.index as usize);
                    self.log.push(entry);
                }
                None => {
                    self.log.push(entry);
                }
                _ => {
                    // Entry already exists and matches, do nothing
                }
            }
        }

        // (Paper 5.3 Step 5)
        if args.leader_commit > self.committed_index {
            // committed_index = min(leader_commit, index of last new entry)
            self.committed_index = std::cmp::min(args.leader_commit, last_new_entry_index);
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
        }
    }

    fn has_matching_prev_entry(&self, index: u64, term: u64) -> bool {
        self.log
            .get(index as usize)
            .is_some_and(|entry| entry.term == term)
    }

    fn apply_leader_term_and_step_down(&mut self, leader_term: u64) {
        if leader_term > self.current_term {
            self.current_term = leader_term;
            self.voted_for = None;
            self.state = NodeState::Follower;
            return;
        }

        if self.state != NodeState::Follower {
            self.state = NodeState::Follower;
        }
    }

    fn reject_append_entries(&self) -> AppendEntriesReply {
        AppendEntriesReply {
            term: self.current_term,
            success: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state() {
        let node = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);
        assert_eq!(node.state, NodeState::Follower);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.voted_for, None);
        assert_eq!(node.log.len(), 1);
        assert_eq!(node.committed_index, 0);
    }

    // follower tests
    #[test]
    fn test_follower_updates_committed_index_on_append_entries_arg() {
        let mut follower = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);
        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });
        follower.log.push(LogEntry {
            term: 1,
            index: 2,
            command: "cmd1".to_string(),
        });

        let args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 2, // The index of the entry preceding the new ones (which is empty here)
            prev_log_term: 1,  // The term of the entry at prev_log_index
            entries: vec![],   // No new entries, just a heartbeat
            leader_commit: 2,  // The leader's commit index
        };

        follower.handle_append_entries(args);

        assert_eq!(follower.committed_index, 2);
    }

    #[test]
    fn test_follower_rejects_append_entries_when_prev_log_index_is_missing() {
        let mut follower = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);

        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });

        let args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 2, // error happens here, the follower's log only has index 1,
            prev_log_term: 1,
            entries: vec![], // No new entries, just a heartbeat
            leader_commit: 0,
        };

        let reply = follower.handle_append_entries(args);
        assert_eq!(reply.success, false);
    }

    #[test]
    fn test_follower_rejects_append_entries_when_prev_log_term_mismatch() {
        let mut follower = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);

        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });

        let args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 1,
            prev_log_term: 2, // error happens here, the term at index 1 is 1, not 2
            entries: vec![],  // No new entries, just a heartbeat
            leader_commit: 0,
        };

        let reply = follower.handle_append_entries(args);
        assert_eq!(reply.success, false);
    }

    #[test]
    fn test_follower_updates_term_when_receiving_higher_term_append_entries() {
        let mut follower = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);
        follower.current_term = 1;
        follower.voted_for = Some("node-1".to_string());

        let args = AppendEntriesArgs {
            term: 2,
            leader_id: "node-2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let reply = follower.handle_append_entries(args);

        assert_eq!(reply.success, true);
        assert_eq!(follower.current_term, 2);
        assert_eq!(follower.voted_for, None);
    }

    #[test]
    fn test_follower_appends_and_overwrites_logs() {
        let mut follower = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);
        // Log state: [ (0,0,sentinel), (1,1,"cmd1"), (1,2,"old_cmd2") ]
        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });
        follower.log.push(LogEntry {
            term: 1,
            index: 2,
            command: "old_cmd2".to_string(),
        });

        // Leader sends: prev_log_index=1, prev_log_term=1
        // Entries: [ (2,2,"new_cmd2"), (2,3,"new_cmd3") ]
        // This should:
        // 1. Match at index 1.
        // 2. Discover conflict at index 2 (Term 1 != Term 2).
        // 3. Remove index 2 and everything after.
        // 4. Append the two new entries.
        let args = AppendEntriesArgs {
            term: 2,
            leader_id: "node-2".to_string(),
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![
                LogEntry {
                    term: 2,
                    index: 2,
                    command: "new_cmd2".to_string(),
                },
                LogEntry {
                    term: 2,
                    index: 3,
                    command: "new_cmd3".to_string(),
                },
            ],
            leader_commit: 3,
        };

        let reply = follower.handle_append_entries(args);

        assert_eq!(reply.success, true);
        // Final log should be: [ sentinel, (1,1), (2,2), (2,3) ]
        assert_eq!(follower.log.len(), 4);
        assert_eq!(follower.log[2].term, 2);
        assert_eq!(follower.log[2].command, "new_cmd2");
        assert_eq!(follower.log[3].term, 2);
        assert_eq!(follower.log[3].index, 3);

        // committed_index should be min(leader_commit, last_new_entry_index)
        // min(3, 3) = 3
        assert_eq!(follower.committed_index, 3);
    }

    // leader tests
    #[test]
    fn test_leader_steps_down_when_receiving_higher_term_append_entries() {
        let mut leader = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);

        leader.state = NodeState::Leader {
            next_indices: HashMap::new(),
            match_indices: HashMap::new(),
        };
        leader.current_term = 1;

        let args = AppendEntriesArgs {
            term: 2, // Higher term than the leader's current term
            leader_id: "node-2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        leader.handle_append_entries(args);

        assert_eq!(leader.state, NodeState::Follower);
        assert_eq!(leader.current_term, 2);
        assert_eq!(leader.voted_for, None);
    }

    #[test]
    fn test_leader_does_not_update_committed_index_from_append_entries() {
        let mut leader = RaftNode::new("node-1".to_string(), vec!["node-2".to_string()]);

        let mut next_indices = HashMap::new();
        let mut match_indices = HashMap::new();
        next_indices.insert("node-2".to_string(), 1);
        match_indices.insert("node-2".to_string(), 0);

        leader.state = NodeState::Leader {
            next_indices,
            match_indices,
        };

        leader.current_term = 2;
        leader.committed_index = 1;

        // Crazy Scenario: Network partition happened. Node 2 becomes new leader but is still at term 2.
        let args = AppendEntriesArgs {
            term: 2,
            leader_id: "node-2".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 5, // A higher commit index from a conflicting leader
        };

        leader.handle_append_entries(args);

        // The leader should not update its committed index based on the AppendEntries from another leader
        assert_eq!(leader.committed_index, 1);
    }
}
