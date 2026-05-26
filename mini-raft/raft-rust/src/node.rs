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

    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> (AppendEntriesReply, Vec<SideEffect>) {
        let mut side_effects = Vec::new();

        // reject lagacy leader (Paper 5.1)
        if args.term < self.current_term {
            return (self.reject_append_entries(), side_effects);
        }

        // Paper 5.1
        self.maybe_step_down(args.term);

        // Paper 5.2
        // Case: arg.term == self.current_term and I am candidate => step down to follower
        self.state = NodeState::Follower;
        side_effects.push(SideEffect::ResetElectionTimer);

        if !self.has_matching_prev_entry(args.prev_log_index, args.prev_log_term) {
            return (self.reject_append_entries(), side_effects);
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

        let reply = AppendEntriesReply {
            term: self.current_term,
            success: true,
        };

        (reply, side_effects)
    }

    pub fn handle_request_vote(
        &mut self,
        candidate_args: RequestVoteArgs,
    ) -> (RequestVoteReply, Vec<SideEffect>) {
        let mut side_effects = Vec::new();

        if candidate_args.term < self.current_term {
            return (
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                },
                side_effects,
            );
        }

        self.maybe_step_down(candidate_args.term);

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
            self.voted_for = Some(candidate_args.candidate_id);
            side_effects.push(SideEffect::ResetElectionTimer);

            (
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: true,
                },
                side_effects,
            )
        } else {
            (
                RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                },
                side_effects,
            )
        }
    }

    pub fn handle_timeout(&mut self) -> Vec<SideEffect> {
        let mut side_effects = Vec::new();

        // Safty check: This should never happen for a leader
        if let NodeState::Leader { .. } = self.state {
            return side_effects;
        }

        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());

        side_effects.push(SideEffect::ResetElectionTimer);

        let last_log = self
            .log
            .last()
            .expect("Log should at least have a sentinel entry");
        let request_vote_args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: last_log.index,
            last_log_term: last_log.term,
        };

        side_effects.push(SideEffect::BroadcastRequestVote(request_vote_args));

        side_effects
    }

    fn has_matching_prev_entry(&self, index: u64, term: u64) -> bool {
        self.log
            .get(index as usize)
            .is_some_and(|entry| entry.term == term)
    }

    fn maybe_step_down(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
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
    fn test_request_vote_rejects_old_term() {
        let mut node = setup_node();
        node.current_term = 2;

        let args = RequestVoteArgs {
            term: 1,
            candidate_id: "candidate-A".to_string(),
            last_log_index: 0,
            last_log_term: 0,
        };

        let (reply, side_effects) = node.handle_request_vote(args);
        assert_eq!(reply.vote_granted, false);
        assert_eq!(reply.term, 2);
        assert!(side_effects.is_empty());
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

        let (reply, side_effects) = node.handle_request_vote(args);

        assert_eq!(reply.vote_granted, false);
        assert_eq!(reply.term, 3);
        assert_eq!(node.voted_for, Some("candidate-A".to_string()));
        assert!(side_effects.is_empty());
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

        let (voter_reply, side_effects) = voter.handle_request_vote(candidate_args);
        assert_eq!(voter_reply.vote_granted, false);
        assert_eq!(voter.voted_for, None);
        assert!(side_effects.is_empty());
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

        let (voter_reply, side_effects) = voter.handle_request_vote(candidate_args);
        assert_eq!(voter_reply.vote_granted, false);
        assert_eq!(voter.voted_for, None);
        assert!(side_effects.is_empty());
    }

    #[test]
    fn test_timeout_triggers_election() {
        let mut follower = setup_node();
        follower.current_term = 1;
        follower.log.push(LogEntry {
            term: 1,
            index: 1,
            command: "cmd1".to_string(),
        });

        let side_effects = follower.handle_timeout();

        let new_candidate = follower;

        assert_eq!(new_candidate.state, NodeState::Candidate);
        assert_eq!(new_candidate.current_term, 2);
        assert_eq!(new_candidate.voted_for, Some("node-1".to_string())); // vote for self
        assert!(side_effects.contains(&SideEffect::ResetElectionTimer));
        let found_broadcast = side_effects
            .iter()
            .any(|se| matches!(se, SideEffect::BroadcastRequestVote(_)));
        assert!(found_broadcast);
    }
}
