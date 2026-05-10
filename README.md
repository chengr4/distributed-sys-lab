# Distributed System Lab

## Usage

```bash
make test
make test-race
make test-coverage
```

## Mini RPC

Each Node has following functions:

1. `store(name, value)`
2. `read(name)`
3. `add(num1, num2)`
4. `getTime()`