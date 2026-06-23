use rand::Rng;
use std::env;
use std::io::{Write, BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use raft_rust::node::RaftNode;
use raft_rust::protocol::{Message, SideEffect, AppendEntriesArgs, RequestVoteArgs, AppendEntriesReply, RequestVoteReply};
use serde_json;

struct EngineState {
    election_tick_count: u64,
    heartbeat_tick_count: u64,
    election_timeout_limit: u64,
}

impl EngineState {
    fn new() -> Self {
        let mut rng = rand::thread_rng();
        Self {
            election_tick_count: 0,
            heartbeat_tick_count: 0,
            election_timeout_limit: rng.gen_range(30..150), // 1.5s to 7.5s (with 50ms ticks)
        }
    }

    fn reset_election_timer(&mut self) {
        let mut rng = rand::thread_rng();
        self.election_tick_count = 0;
        self.election_timeout_limit = rng.gen_range(30..150);
    }

    fn reset_heartbeat_timer(&mut self) {
        self.heartbeat_tick_count = 0;
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: raft-rust <id> <port> <relay_addr> <peer1_id> <peer2_id> ...");
        return;
    }

    let id = args[1].clone();
    let port = args[2].parse::<u16>().expect("Invalid port");
    let relay_addr = args[3].clone();
    let peers: Vec<String> = args[4..].to_vec();

    let node = Arc::new(Mutex::new(RaftNode::new(id.clone(), peers.clone())));
    let engine_state = Arc::new(Mutex::new(EngineState::new()));
    
    // Create a channel for outgoing messages to avoid blocking the main loop
    let (tx, rx) = mpsc::channel::<Message>();
    let relay_addr_sender = relay_addr.clone();
    
    thread::spawn(move || {
        for msg in rx {
            let r_addr = relay_addr_sender.clone();
            thread::spawn(move || {
                send_to_relay_raw(&r_addr, msg);
            });
        }
    });

    // Start TCP Listener for incoming messages from Relay
    let node_clone = Arc::clone(&node);
    let engine_state_clone = Arc::clone(&engine_state);
    let listen_addr = format!("0.0.0.0:{}", port);
    let relay_addr_clone = relay_addr.clone();
    let tx_clone = tx.clone();
    
    thread::spawn(move || {
        let addr: std::net::SocketAddr = listen_addr.parse().expect("Invalid listen address");
        let socket = socket2::Socket::new(socket2::Domain::for_address(addr), socket2::Type::STREAM, None).expect("Failed to create socket");
        
        // SO_REUSEADDR and SO_REUSEPORT allow immediate rebinding of a port.
        socket.set_reuse_address(true).expect("Failed to set SO_REUSEADDR");
        #[cfg(not(windows))]
        socket.set_reuse_port(true).expect("Failed to set SO_REUSEPORT");
        
        socket.bind(&addr.into()).expect("Failed to bind port");
        socket.listen(128).expect("Failed to listen");
        
        let listener: TcpListener = socket.into();
        
        for stream in listener.incoming() {
            if let Ok(stream) = stream {
                let n = Arc::clone(&node_clone);
                let e = Arc::clone(&engine_state_clone);
                let t = tx_clone.clone();
                let r = relay_addr_clone.clone();
                thread::spawn(move || handle_connection(stream, n, e, t, r));
            }
        }
    });

    // Startup Jitter: Break lockstep if multiple nodes start at the same time
    let mut rng = rand::thread_rng();
    let jitter = rng.gen_range(0..500);
    thread::sleep(Duration::from_millis(jitter));

    // Main Tick Loop: Simulate randomized election timeout and periodic heartbeats
    let heartbeat_interval = 6; // 300ms (with 50ms ticks)

    loop {
        thread::sleep(Duration::from_millis(50));
        
        let mut n = node.lock().unwrap();
        let mut e = engine_state.lock().unwrap();
        
        // 1. Handle Leader Heartbeats
        if matches!(n.state, raft_rust::protocol::NodeState::Leader { .. }) {
            e.heartbeat_tick_count += 1;
            if e.heartbeat_tick_count >= heartbeat_interval {
                let effects = n.handle_heartbeat_timeout();
                execute_effects(&mut n, &mut e, &tx, effects);
            }
            // Leaders don't trigger election timeouts
            e.election_tick_count = 0;
            continue; 
        }

        // 2. Handle Follower/Candidate Election Timeouts
        e.election_tick_count += 1;
        if e.election_tick_count >= e.election_timeout_limit {
            let effects = n.handle_election_timeout();
            execute_effects(&mut n, &mut e, &tx, effects);
        }
    }
}

fn handle_connection(stream: TcpStream, node: Arc<Mutex<RaftNode>>, engine_state: Arc<Mutex<EngineState>>, tx: mpsc::Sender<Message>, _relay_addr: String) {
    let reader = BufReader::new(&stream);
    for line in reader.lines() {
        if let Ok(line) = line {
            let msg: Message = match serde_json::from_str(&line) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("JSON Parse Error: {}", e);
                    continue;
                }
            };

            let mut n = node.lock().unwrap();
            let mut e = engine_state.lock().unwrap();
            
            // Convert Value payload back to bytes for typed deserialization
            let payload_bytes = serde_json::to_vec(&msg.payload).unwrap();

            let effects = match msg.r#type.as_str() {
                "AppendEntries" => {
                    let args: AppendEntriesArgs = serde_json::from_slice(&payload_bytes).expect("Payload mismatch: AppendEntries");
                    let (reply, effects) = n.handle_append_entries(args);
                    let reply_payload = serde_json::to_value(&reply).unwrap();
                    let _ = tx.send(Message {
                        from: n.id.clone(),
                        to: msg.from.clone(),
                        r#type: "AppendReply".to_string(),
                        payload: reply_payload,
                    });
                    effects
                }
                "RequestVote" => {
                    let args: RequestVoteArgs = serde_json::from_slice(&payload_bytes).expect("Payload mismatch: RequestVote");
                    let (reply, effects) = n.handle_request_vote(args);
                    let reply_payload = serde_json::to_value(&reply).unwrap();
                    let _ = tx.send(Message {
                        from: n.id.clone(),
                        to: msg.from.clone(),
                        r#type: "VoteReply".to_string(),
                        payload: reply_payload,
                    });
                    effects
                }
                "AppendReply" => {
                    let reply: AppendEntriesReply = serde_json::from_slice(&payload_bytes).expect("Payload mismatch: AppendReply");
                    n.handle_append_entries_reply(msg.from.clone(), reply)
                }
                "VoteReply" => {
                    let reply: RequestVoteReply = serde_json::from_slice(&payload_bytes).expect("Payload mismatch: VoteReply");
                    n.handle_request_vote_reply(msg.from.clone(), reply)
                }
                "ClientRequest" => {
                    let args: raft_rust::protocol::ClientRequestArgs = serde_json::from_slice(&payload_bytes).expect("Payload mismatch: ClientRequest");
                    let (reply, effects) = n.handle_client_request(args);
                    let reply_payload = serde_json::to_value(&reply).unwrap();
                    let _ = tx.send(Message {
                        from: n.id.clone(),
                        to: msg.from.clone(),
                        r#type: "ClientReply".to_string(),
                        payload: reply_payload,
                    });
                    effects
                }
                _ => vec![],
            };

            execute_effects(&mut n, &mut e, &tx, effects);
        }
    }
}

fn execute_effects(node: &mut RaftNode, engine_state: &mut EngineState, tx: &mpsc::Sender<Message>, effects: Vec<SideEffect>) {
    for effect in effects {
        match effect {
            SideEffect::LogMessage(m) => println!("{}", m),
            SideEffect::ApplyEntry { index, command } => {
                // In a real system, this is where you'd write to a database or trigger business logic.
                // For this lab, we acknowledge the application and could update a local view.
                println!("[Node={}] *** STATE MACHINE APPLY *** Index: {}, Command: '{}'", node.id, index, command);
            }
            SideEffect::SendAppendEntries(target, args) => {
                let payload = serde_json::to_value(&args).unwrap();
                let _ = tx.send(Message {
                    from: node.id.clone(),
                    to: target,
                    r#type: "AppendEntries".to_string(),
                    payload,
                });
            }
            SideEffect::BroadcastAppendEntries(args) => {
                let payload = serde_json::to_value(&args).unwrap();
                for peer in &node.peers {
                    let _ = tx.send(Message {
                        from: node.id.clone(),
                        to: peer.clone(),
                        r#type: "AppendEntries".to_string(),
                        payload: payload.clone(),
                    });
                }
            }
            SideEffect::BroadcastRequestVote(args) => {
                let payload = serde_json::to_value(&args).unwrap();
                for peer in &node.peers {
                    let _ = tx.send(Message {
                        from: node.id.clone(),
                        to: peer.clone(),
                        r#type: "RequestVote".to_string(),
                        payload: payload.clone(),
                    });
                }
            }
            SideEffect::ResetElectionTimer => {
                engine_state.reset_election_timer();
            }
            SideEffect::ResetHeartbeatTimer => {
                engine_state.reset_heartbeat_timer();
            }
        }
    }
}

fn send_to_relay_raw(relay_addr: &str, msg: Message) {
    match TcpStream::connect(relay_addr) {
        Ok(mut stream) => {
            let json = serde_json::to_string(&msg).unwrap();
            let _ = writeln!(stream, "{}", json);
        }
        Err(e) => eprintln!("Failed to connect to Relay at {}: {}", relay_addr, e),
    }
}
