use std::collections::{HashMap, HashSet};

use crate::protocol::*;

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

    fn get_state_str(&self) -> &str {
        match &self.state {
            NodeState::Follower => "Follower",
            NodeState::Candidate { .. } => "Candidate",
            NodeState::Leader { .. } => "Leader",
        }
    }

    fn build_log_event(&self, message: &str) -> SideEffect {
        SideEffect::LogMessage(format!(
            "[T={}][Node={}][{}] {}",
            self.current_term,
            self.id,
            self.get_state_str(),
            message
        ))
    }

    // Only Follower allow to have this behavior
    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Vec<SideEffect>) {
        let mut side_effects = Vec::new();

        // reject lagacy leader (Paper 5.1)
        if args.term < self.current_term {
            side_effects.push(self.build_log_event(&format!("Rejected AppendEntries from Node {} (Term {}): Term too old", args.leader_id, args.term)));
            return (self.reject_append_entries(), side_effects);
        }

        // Paper 5.1
        self.maybe_step_down(args.term, &mut side_effects);

        side_effects.push(self.build_log_event(&format!("Received AppendEntries from Node {} (Term {}) with {} entries", args.leader_id, args.term, args.entries.len())));

        // Paper 5.2
        // Case: arg.term == self.current_term and I am candidate => step down to follower
        if matches!(self.state, NodeState::Candidate { .. }) {
             side_effects.push(self.build_log_event("Recognized Leader -> Stepping down to Follower"));
        }
        self.state = NodeState::Follower;
        side_effects.push(SideEffect::ResetElectionTimer);

        if !self.has_matching_prev_entry(args.prev_log_index, args.prev_log_term) {
            side_effects.push(self.build_log_event(&format!("Rejected AppendEntries: Consistency check failed at Index {}", args.prev_log_index)));
            return (self.reject_append_entries(), side_effects);
        }

        // Add new entries and handle conflicts (Paper 5.3 Step 3 & 4)
        // for heartsbeat case that args.entries is empty
        let mut last_new_entry_index = args.prev_log_index;
        for entry in args.entries {
            last_new_entry_index = entry.index;
            match self.log.get(entry.index as usize) {
                Some(existing) if existing.term != entry.term => {
                    // Conflict detected, delete the existing entry and all that follow it
                    side_effects.push(self.build_log_event(&format!("Log conflict at Index {}: truncating log", entry.index)));
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
            let old_commit = self.committed_index;
            // committed_index = min(leader_commit, index of last new entry)
            self.committed_index = std::cmp::min(args.leader_commit, last_new_entry_index);
            if self.committed_index > old_commit {
                side_effects.push(self.build_log_event(&format!("Updated committed_index to {}", self.committed_index)));
            }
        }

        let reply = AppendEntriesReply {
            term: self.current_term,
            success: true,
            match_index: last_new_entry_index,
        };

        (reply, side_effects)
    }

    pub fn handle_request_vote(
        &mut self,
        candidate_args: RequestVoteArgs,
    ) -> (RequestVoteReply, Vec<SideEffect>) {
        let mut side_effects = Vec::new();

        if candidate_args.term < self.current_term {
            side_effects.push(self.build_log_event(&format!("Rejected RequestVote from Node {} (Term {}): Term too old", candidate_args.candidate_id, candidate_args.term)));
            return (
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                },
                side_effects,
            );
        }

        self.maybe_step_down(candidate_args.term, &mut side_effects);

        let voter_last_log = self.log.last().unwrap();

        // Paper 5.4.1
        let is_candidate_at_least_as_up_to_date = candidate_args.last_log_term
            > voter_last_log.term
            || (candidate_args.last_log_term == voter_last_log.term
                && candidate_args.last_log_index >= voter_last_log.index);

        let voter_can_vote_for_candidate = match &self.voted_for {
            None => true,
            Some(id) if *id == candidate_args.candidate_id => true,
            _ => false,
        };

        if voter_can_vote_for_candidate && is_candidate_at_least_as_up_to_date {
            self.voted_for = Some(candidate_args.candidate_id.clone());
            side_effects.push(SideEffect::ResetElectionTimer);
            side_effects.push(self.build_log_event(&format!("Voted for Node {}", candidate_args.candidate_id)));

            (
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: true,
                },
                side_effects,
            )
        } else {
            let reason = if !voter_can_vote_for_candidate { "Already voted" } else { "Log not up-to-date" };
            side_effects.push(self.build_log_event(&format!("Rejected RequestVote from Node {}: {}", candidate_args.candidate_id, reason)));
            (
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                },
                side_effects,
            )
        }
    }

    /// Heartbeat Timeout (Leader only)
    pub fn handle_heartbeat_timeout(&mut self) -> Vec<SideEffect> {
        let mut side_effects = Vec::new();

        if let NodeState::Leader { next_indices, .. } = &self.state {
            side_effects.push(self.build_log_event("Heartbeat Timeout -> Broadcasting heartbeats"));
            for peer in &self.peers {
                let next_index = next_indices.get(peer).copied().unwrap_or(1);
                if let Some(args) = self.build_append_entries_args(next_index) {
                    side_effects.push(SideEffect::SendAppendEntries(peer.clone(), args));
                }
            }
            side_effects.push(SideEffect::ResetHeartbeatTimer);
        }

        side_effects
    }

    /// Election Timeout (Follower/Candidate only)
    pub fn handle_election_timeout(&mut self) -> Vec<SideEffect> {
        let mut side_effects = Vec::new();

        // Safety check: Leaders should not trigger election timeouts
        if let NodeState::Leader { .. } = self.state {
            return side_effects;
        }

        let old_state_str = self.get_state_str().to_string();
        let mut votes_received = HashSet::new();
        votes_received.insert(self.id.clone()); // vote for self

        self.state = NodeState::Candidate { votes_received };
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());

        side_effects.push(self.build_log_event(&format!("Election Timeout -> Transition: {} -> Candidate (Term {})", old_state_str, self.current_term)));

        side_effects.push(SideEffect::ResetElectionTimer);

        let candidate_last_log = self
            .log
            .last()
            .expect("Log should at least have a sentinel entry");
        let request_vote_args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: candidate_last_log.index,
            last_log_term: candidate_last_log.term,
        };

        side_effects.push(self.build_log_event("Broadcasting RequestVote"));
        side_effects.push(SideEffect::BroadcastRequestVote(request_vote_args));

        side_effects
    }

    // Behavior: Candidate
    pub fn handle_request_vote_reply(
        &mut self,
        from: String,
        reply: RequestVoteReply,
    ) -> Vec<SideEffect> {
        let mut side_effects = Vec::new();

        if self.maybe_step_down(reply.term, &mut side_effects) {
            return side_effects;
        }

        let mut won_election = false;
        if let NodeState::Candidate { votes_received } = &mut self.state {
            if reply.term == self.current_term && reply.vote_granted {
                votes_received.insert(from.clone());
                let votes_count = votes_received.len();
                side_effects.push(self.build_log_event(&format!("Received vote from Node {} (Total votes: {})", from, votes_count)));

                // Majority = (N / 2) + 1
                let total_nodes = self.peers.len() + 1;
                if votes_count > total_nodes / 2 {
                    won_election = true;
                }
            } else if !reply.vote_granted {
                side_effects.push(self.build_log_event(&format!("Vote denied by Node {} (Term {})", from, reply.term)));
            }
        }

        if won_election {
            let old_state_str = self.get_state_str().to_string();
            let last_log = self.log.last().unwrap();
            let last_log_index = last_log.index;
            let last_log_term = last_log.term;

            let mut next_indices = HashMap::new();
            let mut match_indices = HashMap::new();

            for peer in &self.peers {
                next_indices.insert(peer.clone(), last_log_index + 1);
                match_indices.insert(peer.clone(), 0);
            }

            self.state = NodeState::Leader {
                next_indices,
                match_indices,
            };

            side_effects.push(self.build_log_event(&format!("Won election -> Transition: {} -> Leader", old_state_str)));

            // Send the first heartbeat immediately after becoming leader
            let heartbeat_args = AppendEntriesArgs {
                term: self.current_term,
                leader_id: self.id.clone(),
                prev_log_index: last_log_index,
                prev_log_term: last_log_term,
                entries: vec![],
                leader_commit: self.committed_index,
            };
            side_effects.push(SideEffect::BroadcastAppendEntries(heartbeat_args));
            // Initial leadership resets the heartbeat timer
            side_effects.push(SideEffect::ResetHeartbeatTimer);
        }
        
        side_effects
    }

    pub fn handle_append_entries_reply(
        &mut self,
        from: String,
        follower_reply: AppendEntriesReply,
    ) -> Vec<SideEffect> {
        let mut side_effects = Vec::new();

        if self.maybe_step_down(follower_reply.term, &mut side_effects) {
            return side_effects;
        }

        // Defensive check: Only process replies that belong to the current term's leadership.
        if follower_reply.term != self.current_term {
            return side_effects;
        }

        if let NodeState::Leader {
            next_indices,
            match_indices,
        } = &mut self.state
        {
            if follower_reply.success {
                let current_follower_next_index_in_leader =
                    next_indices.get(&from).copied().unwrap_or(1);
                let new_match_index = follower_reply.match_index;
                match_indices.insert(from.clone(), new_match_index);

                // Defensive check: Monotonicity Guard.
                if new_match_index + 1 > current_follower_next_index_in_leader {
                    next_indices.insert(from.clone(), new_match_index + 1);
                }
                
                side_effects.push(self.build_log_event(&format!("Node {} updated match_index to {}", from, new_match_index)));
            // TODO: Implement Figure 8 committed_index update
            } else {
                let current_follower_next_index_in_leader =
                    next_indices.get(&from).copied().unwrap_or(1);
                let retry_next_index =
                    if follower_reply.match_index < current_follower_next_index_in_leader {
                        follower_reply.match_index + 1
                    } else {
                        current_follower_next_index_in_leader
                            .saturating_sub(1)
                            .max(1)
                    };

                next_indices.insert(from.clone(), retry_next_index);
                side_effects.push(self.build_log_event(&format!("Node {} rejected AppendEntries, retrying with next_index {}", from, retry_next_index)));

                if let Some(leader_retry_args) = self.build_append_entries_args(retry_next_index) {
                    side_effects.push(SideEffect::SendAppendEntries(from, leader_retry_args));
                }
            }
        }

        side_effects
    }

    fn has_matching_prev_entry(&self, index: u64, term: u64) -> bool {
        self.log
            .get(index as usize)
            .is_some_and(|entry| entry.term == term)
    }

    fn maybe_step_down(&mut self, term: u64, side_effects: &mut Vec<SideEffect>) -> bool {
        if term > self.current_term {
            let old_term = self.current_term;
            let old_state_str = self.get_state_str().to_string();
            self.current_term = term;
            self.voted_for = None;
            self.state = NodeState::Follower;
            side_effects.push(self.build_log_event(&format!(
                "Term updated ({} -> {}) -> Transition: {} -> Follower",
                old_term, term, old_state_str
            )));
            return true;
        }
        false
    }

    fn reject_append_entries(&self) -> AppendEntriesReply {
        let hint_index = self.log.last().map(|e| e.index).unwrap_or(0);
        AppendEntriesReply {
            term: self.current_term,
            success: false,
            match_index: hint_index,
        }
    }

    fn build_append_entries_args(&self, next_index: u64) -> Option<AppendEntriesArgs> {
        if next_index == 0 {
            return None;
        }
        let prev_log_index = next_index - 1;
        let prev_log_term = self
            .log
            .get(prev_log_index as usize)
            .map(|e| e.term)
            .unwrap_or(0);

        let entries = if (next_index as usize) < self.log.len() {
            self.log[next_index as usize..].to_vec()
        } else {
            vec![]
        };

        Some(AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.committed_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn setup_node() -> RaftNode {
        RaftNode::new(
            "node-1".to_string(),
            vec!["node-2".to_string(), "node-3".to_string()],
        )
    }

    #[test]
    fn test_initial_state() {
        let node = setup_node();
        assert_eq!(node.state, NodeState::Follower);
        assert_eq!(node.current_term, 0);
        assert_eq!(node.voted_for, None);
        assert_eq!(node.log.len(), 1);
        assert_eq!(node.committed_index, 0);
    }

    // follower tests
    #[test]
    fn test_follower_updates_committed_index_on_append_entries_arg() {
        let mut follower = setup_node();
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

        let leader_args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 2,
        };

        follower.handle_append_entries(leader_args);

        assert_eq!(follower.committed_index, 2);
    }

    #[test]
    fn test_follower_rejects_append_entries_when_prev_log_index_is_missing() {
        let mut follower = setup_node();

        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });

        let leader_args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 0,
        };

        let (reply, _) = follower.handle_append_entries(leader_args);
        assert_eq!(reply.success, false);
    }

    #[test]
    fn test_follower_rejects_append_entries_when_prev_log_term_mismatch() {
        let mut follower = setup_node();

        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });

        let leader_args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        };

        let (reply, _) = follower.handle_append_entries(leader_args);
        assert_eq!(reply.success, false);
    }

    #[test]
    fn test_follower_updates_term_when_receiving_higher_term_append_entries() {
        let mut follower = setup_node();
        follower.current_term = 1;
        follower.voted_for = Some("node-1".to_string());

        let leader_args = AppendEntriesArgs {
            term: 2,
            leader_id: "node-2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let (reply, _) = follower.handle_append_entries(leader_args);

        assert_eq!(reply.success, true);
        assert_eq!(follower.current_term, 2);
        assert_eq!(follower.voted_for, None);
    }

    #[test]
    fn test_follower_appends_and_overwrites_logs() {
        let mut follower = setup_node();
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

        let leader_args = AppendEntriesArgs {
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

        let (reply, _) = follower.handle_append_entries(leader_args);

        assert_eq!(reply.success, true);
        assert_eq!(follower.log.len(), 4);
        assert_eq!(follower.log[2].term, 2);
        assert_eq!(follower.log[2].command, "new_cmd2");
        assert_eq!(follower.log[3].term, 2);
        assert_eq!(follower.log[3].index, 3);
        assert_eq!(follower.committed_index, 3);
    }

    #[test]
    fn test_follower_resets_timer_on_valid_append_entries() {
        let mut follower = setup_node();
        follower.current_term = 1;

        let leader_args = AppendEntriesArgs {
            term: 1,
            leader_id: "node-2".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let (_reply, side_effects) = follower.handle_append_entries(leader_args);

        assert!(side_effects.contains(&SideEffect::ResetElectionTimer));
    }

    #[test]
    fn test_leader_steps_down_when_receiving_higher_term_append_entries() {
        let mut leader = setup_node();

        leader.state = NodeState::Leader {
            next_indices: HashMap::new(),
            match_indices: HashMap::new(),
        };
        leader.current_term = 1;

        let args = AppendEntriesArgs {
            term: 2,
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
        let mut leader = setup_node();

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

        let args = AppendEntriesArgs {
            term: 2,
            leader_id: "node-2".to_string(),
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 5,
        };

        leader.handle_append_entries(args);

        assert_eq!(leader.committed_index, 1);
    }

    #[test]
    fn test_leader_updates_indices_on_successful_append_entries_reply() {
        let mut leader = setup_node();

        let mut next_indices = HashMap::new();
        let mut match_indices = HashMap::new();
        next_indices.insert("node-2".to_string(), 1);
        match_indices.insert("node-2".to_string(), 0);

        leader.state = NodeState::Leader {
            next_indices,
            match_indices,
        };

        leader.current_term = 2;

        // Assuming leader has index 1 and 2 in its log
        leader.log.push(LogEntry {
            term: 2,
            index: 1,
            command: "c1".into(),
        });
        leader.log.push(LogEntry {
            term: 2,
            index: 2,
            command: "c2".into(),
        });

        let follower_reply = AppendEntriesReply {
            term: 2,
            success: true,
            match_index: 2,
        };

        leader.handle_append_entries_reply("node-2".to_string(), follower_reply);

        if let NodeState::Leader {
            next_indices,
            match_indices,
        } = &leader.state
        {
            assert_eq!(next_indices.get("node-2"), Some(&3)); // match_index + 1
            assert_eq!(match_indices.get("node-2"), Some(&2));
        } else {
            panic!("Should be leader");
        }
    }

    #[test]
    fn test_leader_decrements_next_index_and_retries_on_failed_append_entries_reply() {
        let mut leader = setup_node();
        let mut next_indices = HashMap::new();
        let mut match_indices = HashMap::new();

        next_indices.insert("node-2".to_string(), 5);
        match_indices.insert("node-2".to_string(), 0);

        leader.state = NodeState::Leader {
            next_indices,
            match_indices,
        };
        leader.current_term = 2;

        for i in 1..=5 {
            leader.log.push(LogEntry {
                term: 2,
                index: i,
                command: format!("cmd{}", i),
            });
        }

        let follower_reply = AppendEntriesReply {
            term: 2,
            success: false,
            match_index: 2,
        };

        let side_effects = leader.handle_append_entries_reply("node-2".to_string(), follower_reply);

        if let NodeState::Leader {
            next_indices,
            match_indices: _,
        } = &leader.state
        {
            // match_index + 1
            assert_eq!(next_indices.get("node-2"), Some(&3));
        } else {
            panic!("Should be leader");
        }

        let found_retry = side_effects.iter().any(|se| {
            matches!(
                se,
                SideEffect::SendAppendEntries(target, args)
                if target == "node-2" && args.prev_log_index == 2 && args.entries.len() == 3
            )
        });
        assert!(
            found_retry,
            "Leader should retry SendAppendEntries with correct arguments"
        );
    }

    #[test]
    fn test_request_vote_rejects_old_term() {
        let mut node = setup_node();
        node.current_term = 2;

        let args = RequestVoteArgs {
            term: 1,
            candidate_id: "candidate-A".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };

        let (reply, _) = node.handle_request_vote(args);
        assert_eq!(reply.vote_granted, false);
        assert_eq!(reply.term, 2);
        // side_effect will contain log messages, so we only check the vote
    }

    #[test]
    fn test_request_vote_grants_vote_and_updates_term() {
        let mut node = setup_node();
        node.current_term = 1;

        let args = RequestVoteArgs {
            term: 3,
            candidate_id: "candidate-A".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };

        let (reply, side_effects) = node.handle_request_vote(args);

        assert_eq!(reply.vote_granted, true);
        assert_eq!(reply.term, 3);
        assert_eq!(node.current_term, 3);
        assert_eq!(node.voted_for, Some("candidate-A".to_string()));
        assert_eq!(node.state, NodeState::Follower);
        assert!(side_effects.contains(&SideEffect::ResetElectionTimer));
    }

    #[test]
    fn test_request_vote_rejects_duplicate_vote_in_same_term() {
        let mut node = setup_node();
        node.current_term = 3;
        node.voted_for = Some("candidate-A".to_string());

        let args = RequestVoteArgs {
            term: 3,
            candidate_id: "candidate-B".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };

        let (reply, _) = node.handle_request_vote(args);

        assert_eq!(reply.vote_granted, false);
        assert_eq!(reply.term, 3);
        assert_eq!(node.voted_for, Some("candidate-A".to_string()));
    }

    #[test]
    fn test_request_vote_rejects_if_candidate_log_term_is_older() {
        let mut voter = setup_node();
        voter.current_term = 2;
        voter.log.push(LogEntry {
            term: 2,
            index: 1,
            command: "cmd1".to_string(),
        });

        let candidate_args = RequestVoteArgs {
            term: 3,
            candidate_id: "candidate-A".to_string(),
            last_log_index: 10,
            last_log_term: 1,
        };

        let (voter_reply, _) = voter.handle_request_vote(candidate_args);
        assert_eq!(voter_reply.vote_granted, false);
        assert_eq!(voter.voted_for, None);
    }

    #[test]
    fn test_request_vote_rejects_if_candidate_log_term_same_but_index_shorter() {
        let mut voter = setup_node();
        voter.current_term = 2;
        voter.log.push(LogEntry {
            term: 2,
            index: 1,
            command: "cmd1".to_string(),
        });
        voter.log.push(LogEntry {
            term: 2,
            index: 2,
            command: "cmd1".to_string(),
        });

        let candidate_args = RequestVoteArgs {
            term: 3,
            candidate_id: "candidate-A".to_string(),
            last_log_index: 1,
            last_log_term: 2,
        };

        let (voter_reply, _) = voter.handle_request_vote(candidate_args);
        assert_eq!(voter_reply.vote_granted, false);
        assert_eq!(voter.voted_for, None);
    }

    #[test]
    fn test_election_timeout_triggers_election() {
        let mut follower = setup_node();
        follower.current_term = 1;
        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });

        let side_effects = follower.handle_election_timeout();

        let new_candidate = follower;

        assert!(matches!(new_candidate.state, NodeState::Candidate { .. }));
        assert_eq!(new_candidate.current_term, 2);
        assert_eq!(new_candidate.voted_for, Some("node-1".to_string())); // vote for self
        assert!(side_effects.contains(&SideEffect::ResetElectionTimer));
        let found_broadcast = side_effects
            .iter()
            .any(|se| matches!(se, SideEffect::BroadcastRequestVote(_)));
        assert!(found_broadcast);
    }

    #[test]
    fn test_candidate_becomes_leader_on_majority_votes() {
        // setup_node provides 3 nodes (node-1, node-2, node-3). Majority = 2.
        let mut node = setup_node();

        // 1. Trigger election
        node.handle_election_timeout();
        // Current state: Candidate, Term 1, Votes: {node-1}

        // 2. Receive vote from node-2
        let node2_reply = RequestVoteReply {
            term: 1,
            vote_granted: true,
        };
        let side_effects = node.handle_request_vote_reply("node-2".to_string(), node2_reply);

        // 3. Verify transition
        assert!(matches!(node.state, NodeState::Leader { .. }));
        assert_eq!(node.current_term, 1);

        // 4. Verify Leader state initialization
        if let NodeState::Leader {
            next_indices,
            match_indices,
        } = &node.state
        {
            assert_eq!(next_indices.get("node-2"), Some(&1)); // last_log_index (0) + 1
            assert_eq!(next_indices.get("node-3"), Some(&1));
            assert_eq!(match_indices.get("node-2"), Some(&0));
        } else {
            panic!("Should be leader");
        }

        // 5. Verify immediate heartbeat
        let found_heartbeat = side_effects
            .iter()
            .any(|se| matches!(se, SideEffect::BroadcastAppendEntries(_)));
        assert!(found_heartbeat);
        // Verify ResetHeartbeatTimer on transition
        assert!(side_effects.contains(&SideEffect::ResetHeartbeatTimer));
    }

    #[test]
    fn test_leader_heartbeat_timeout_sends_append_entries() {
        let mut leader = setup_node();
        leader.state = NodeState::Leader {
            next_indices: [("node-2".into(), 1), ("node-3".into(), 1)]
                .into_iter()
                .collect(),
            match_indices: [("node-2".into(), 0), ("node-3".into(), 0)]
                .into_iter()
                .collect(),
        };
        leader.current_term = 1;

        let side_effects = leader.handle_heartbeat_timeout();

        // Check if SendAppendEntries was sent to both peers
        let count = side_effects
            .iter()
            .filter(|se| matches!(se, SideEffect::SendAppendEntries(_, _)))
            .count();
        assert_eq!(count, 2);
        assert!(side_effects.contains(&SideEffect::ResetHeartbeatTimer));
    }
}
