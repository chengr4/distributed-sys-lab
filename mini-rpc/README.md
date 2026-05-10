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

## Submission

- zip file: code, MakeFile, README (Instructions)
- Demo in office hours

## Rubric

- Node A successfully calls 4 functions and receives responses from Node B