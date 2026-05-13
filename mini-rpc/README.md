# Mini RPC

A simple RPC system implemented in Go.

## Features

- **Dual-Role Nodes**: Each process acts as both an RPC Server (offering services) and a Client (invoking services).
- **Interactive CLI**: Every node provides a real-time command-line interface for manual testing and network management.
- **Dynamic Chaining**: Supports multi-hop RPC calls (e.g., A -> B -> C -> D) with dynamic topology configuration via `setNextNode`.
- **Robust Error Handling**: Comprehensive validation for parameters and graceful handling of missing keys or network failures.
- **Timeout Protection**: Integrated timeout mechanism for all remote calls to prevent system hanging and ensure resource availability.
- **Thread-Safe Storage**: Concurrent-safe key-value storage implemented with `RWMutex`.
- **Clean Architecture**: Decoupled design using interfaces (DIP) and adapters for better testability and maintainability (Test Coverage 52.8%).

## How to Use

This project uses a `Makefile` for easy compilation and execution.

### Prerequisites

- [Go](https://go.dev/dl/) (version 1.20 or later recommended)
- `make` utility

### Instructions

1.  **Compile the Project**:
    To compile the code and generate the executable binary:
    ```bash
    make build
    ```
    This creates an executable named `node_app`.

2.  **Run a Single Node**:
    To start a node listening on a specific port (e.g., 1234):
    ```bash
    make run PORT=1234
    ```

3.  **Run All Tests**:
    To execute all unit and integration tests (including concurrency, timeout, and chain tests):
    ```bash
    make test
    ```

### Demo Scenario 1: Basic RPC

This demo proves that Node A can successfully call 4 functions on Node B and receive responses.

1.  **Terminal 1 (Node B):**
    ```bash
    make run PORT=1235
    ```

2.  **Terminal 2 (Node A):**
    ```bash
    make run PORT=1234
    ```

3.  **In Node A's CLI (Terminal 2), execute the following commands:**
    ```text
    > dial localhost:1235
    > store mykey hello_world    # 1. Store function
    > read mykey                 # 2. Read function
    > add 15 27                  # 3. Add function
    > getTime                    # 4. GetTime function
    ```

### Demo Scenario 2: Chain RPC

This demo proves multi-hop forwarding: **Node A -> Node B -> Node C**, with the result returning to Node A. It showcases the ability to remotely configure topology via RPC.

1.  **Start 3 Terminals (Nodes A, B, C):**
    *   Terminal 1 (Node C): `make run PORT=1236`
    *   Terminal 2 (Node B): `make run PORT=1235`
    *   Terminal 3 (Node A): `make run PORT=1234`

2.  **Configure Topology via Node A's CLI (Terminal 3):**
    ```text
    > setNextNode localhost:1235 # Configure Node A to forward to Node B (Local Config)
    > dial localhost:1235        # Connect to Node B
    > setNextNode localhost:1236 # Configure Node B to forward to Node C (Remote Config)
    ```

3.  **Execute Chain Call from Node A:**
    The previous steps configured the chain. To execute from Node A, ensure you are dialed to Node A again:
    ```text
    > dial localhost:1234
    > add 50 25
    ```
    **Expected Result:** `Calculation result: 75`

4.  **How to Verify (Logs):**
    *   **Node A (Terminal 3):** Shows forwarding to Node B.
    *   **Node B (Terminal 2):** Shows forwarding to Node C.
    *   **Node C (Terminal 1):** Executes local `Add` and returns the result.

### Demo Scenario 3: Concurrent Calls

This demo proves that the system can serve multiple requests in parallel and the storage is thread-safe.

1.  **Start a Node:** `make run PORT=1234`
2.  **Run Concurrent Test:**
    In another terminal, run:
    ```bash
    go test -v tests/concurrency_test.go
    ```
    This test triggers **50 simultaneous RPC requests**. 
3.  **Verification:**
    *   The test should pass with: `Successfully completed 50 concurrent requests`.
    *   Check `pkg/storage.go` to see the `sync.RWMutex` implementation which prevents data races.

### Demo Scenario 4: Timeout & Failures

This demo proves the system's robustness against network failures and invalid inputs.

1.  **Timeout Handling (The "Black Hole" Server):**
    To demonstrate a true 3-second timeout (rather than an immediate connection error):
    *   **Start Node A:** `make run PORT=1234`
    *   **Start a Dummy Server (Terminal 2):** Use `nc` to listen on 1235 without responding.
        ```bash
        nc -l 1235
        ```
    *   **In Node A's CLI:** 
        ```text
        > dial localhost:1235
        > add 10 20
        ```
    *   **Observation:** Node A will wait for several seconds and then show: `Call failed: RPC call timed out after ...`.

2.  **Invalid Parameters:**
    *   Try `add hello world` -> Result: `Error: Both arguments must be integers`.
    *   Try `store "" value` -> Result: `Server response: Key cannot be empty`.

3.  **Missing Keys:**
    *   Try `read unknown_key` -> Result: `Error: Key unknown_key not found`.

### How to Manually Test the Service (Dual Role Demo)

To verify the dual-role capability (acting as both Server and Client), follow these steps:

1. **Terminal 1 (Node B):**
   ```bash
   make run PORT=1235
   ```

2. **Terminal 2 (Node A):**
   ```bash
   make run PORT=1234
   # In Node A's CLI:
   > dial localhost:1235
   > store mykey hello
   > read mykey          # Verifies that Node B has stored the data
   ```

3. **Verify Dual Role (Node B to Node A):**
   To prove Node B is also a Client, initiate a call from Node B to Node A:
   ```bash
   # In Node B's CLI:
   > dial localhost:1234
   > getTime             # Node B calls Node A's RPC method
   ```

## TODO

- [x] **Basic RPC Demo** (store, read, add, getTime)
- [x] **Chain RPC Demo** (A -> B -> C -> D)
- [x] **Concurrent Calls Demo** (Thread-safe storage, parallel requests)
- [x] **Timeouts & Failures** (RPC timeout, parameter validation, missing keys)
- [ ] Add forwarding logic for `store` and `read`.
- [ ] Explore and implement **Asynchronous Forwarding**.
- [ ] Add cycle detection or `HopLimit`.

## Codebase Architecture

### Folder

- `tests`: All integration tests for the mini RPC system.
- `pkg`: Core implementation of the mini RPC system

### Clean Architecture

- Domain/Logic:
    - `service.go` (Use case), `requester.go`
    - `storage.go`: It might move to outer layer in the future
- Infrastructure/Adapter:
    - `rpc_adapter.go`: Implements interface
    - `cli.go`: User interface
    - `server.go`

## Takeaway

- `Go()` asynchronous vs `Call()` synchronous RPC calls.