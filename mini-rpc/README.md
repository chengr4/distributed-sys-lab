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

## Submission

- zip file: code, MakeFile, README (Instructions)
- Demo in office hours

## Rubric

- Node A successfully calls 4 functions and receives responses from Node B