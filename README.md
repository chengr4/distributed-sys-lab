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

This demo proves that Node A can successfully call 4 functions on Node B and receive responses:

https://github.com/user-attachments/assets/8b3a7b42-be60-4afe-91ab-2eca38b1ce45

This demo proves multi-hop forwarding: **Node A -> Node B -> Node C**, with the result returning to Node A:

https://github.com/user-attachments/assets/cb4422ad-0c04-446b-9ddc-a8ac0b028015
