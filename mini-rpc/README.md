# Mini RPC

Each Node has:

1. `store(name, value)`
2. `read(name)`
3. `add(num1, num2)`
4. `getTime()`

## Features

- **Dual-Role Nodes**: Each process acts as both an RPC Server (offering services) and a Client (invoking services).
- **Interactive CLI**: Every node provides a real-time command-line interface for manual testing and network management.
- **Dynamic Chaining**: Supports multi-hop RPC calls (e.g., A -> B -> C -> D) with dynamic topology configuration via `setNextNode`.
- **Robust Error Handling**: Comprehensive validation for parameters and graceful handling of missing keys or network failures.
- **Timeout Protection**: Integrated timeout mechanism for all remote calls to prevent system hanging and ensure resource availability.
- **Thread-Safe Storage**: High-performance, concurrent-safe key-value storage implemented with `RWMutex`.
- **Clean Architecture**: Decoupled design using interfaces (DIP) and adapters for superior testability and maintainability.

## Usage

```bash
go test -v
go test -race
go test -cover ./...

go run . -port 1234
```

### How to Manually Test the Service (Dual Role Demo)

To verify the dual-role capability (acting as both Server and Client), follow these steps:

1. **Terminal 1 (Node B):**
   ```bash
   go run . -port 1235
   ```

2. **Terminal 2 (Node A):**
   ```bash
   go run . -port 1234
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

- [ ] Add forwarding logic to `store` and `read`.
- [ ] Explore and implement **Asynchronous Forwarding** to improve chain efficiency.
- [ ] Add cycle detection or `HopLimit` to prevent infinite forwarding.

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