# Mini RPC

Each Node has:

1. `store(name, value)`
2. `read(name)`
3. `add(num1, num2)`
4. `getTime()`

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

- [ ] **Test 2: Chain RPC Demo** (Node A -> Node B -> Node C)
    - [ ] Implement `SetNextNode` to establish chains dynamically.
    - [ ] Add forwarding logic to `Add` (or other methods).
    - [ ] Explore and implement **Asynchronous Forwarding** to improve chain efficiency.
- [ ] **Test 3: Concurrent Calls Demo**
    - [ ] Verify thread-safety of `Storage`.
    - [ ] Add parallel test cases or CLI stress testing.
- [ ] **Test 4: Timeouts & Failures**
    - [ ] Implement timeout mechanism for RPC calls.
    - [ ] Improve error handling for invalid parameters or missing keys.
- [ ] Add cycle detection or `HopLimit` to prevent infinite forwarding.

## Folder Architecture

- `tests`: All integration tests for the mini RPC system.
- `pkg`: Core implementation of the mini RPC system

## Submission

- zip file: code, MakeFile, README (Instructions)
- Demo in office hours

## Takeway

- `Go()` asynchronous vs `Call()` synchronous RPC calls.