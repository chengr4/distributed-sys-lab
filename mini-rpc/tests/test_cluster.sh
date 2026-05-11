#!/bin/bash

# Configuration
BINARY_NAME="node_app"
PORT_A=1234
PORT_B=1235
PORT_C=1236
PORT_D=1237

# 1. Cleanup existing nodes
echo "Cleaning up old processes..."
pkill -f $BINARY_NAME || true
sleep 1

# 2. Build the latest code
echo "Building $BINARY_NAME..."
go build -o $BINARY_NAME .
if [ $? -ne 0 ]; then
    echo "Build failed!"
    exit 1
fi

# 3. Start Node D, C and B in the background
echo "Starting Node D on port $PORT_D..."
./$BINARY_NAME -port $PORT_D > nodeD.log 2>&1 &
PID_D=$!

echo "Starting Node C on port $PORT_C..."
./$BINARY_NAME -port $PORT_C > nodeC.log 2>&1 &
PID_C=$!

echo "Starting Node B on port $PORT_B..."
./$BINARY_NAME -port $PORT_B > nodeB.log 2>&1 &
PID_B=$!

# Give nodes more time to initialize
echo "Waiting for nodes to initialize..."
sleep 2

# 4. Start Node A and Run Automated Tests
echo "------------------------------------------------"
echo "Running Automated Integration Tests (4-Node Cluster)..."
echo "Chain: Node A -> Node B -> Node C -> Node D"
echo "------------------------------------------------"

# Create a temporary file for test output
TEST_LOG="test_execution.log"

# Pipe commands into Node A to setup chain and test
# We use Node A's CLI to dial other nodes to configure the topology
./$BINARY_NAME -port $PORT_A <<EOF > $TEST_LOG 2>&1
# Setup B -> C
dial 127.0.0.1:$PORT_B
setNextNode 127.0.0.1:$PORT_C
# Setup C -> D
dial 127.0.0.1:$PORT_C
setNextNode 127.0.0.1:$PORT_D
# Setup A -> B
dial 127.0.0.1:$PORT_A
setNextNode 127.0.0.1:$PORT_B
# Test Chain Add (A -> B -> C -> D)
add 10 20
# Test local functions on A
getTime
store mykey demo_value
read mykey
exit
EOF

# 5. Verify Results
echo "Verifying results..."
FAILED=0

check_result() {
    if grep -q "$1" $TEST_LOG; then
        echo "[PASS] $2"
    else
        echo "[FAIL] $2"
        FAILED=1
    fi
}

check_result "Successfully connected to 127.0.0.1:$PORT_B" "Setup B->C"
check_result "Successfully connected to 127.0.0.1:$PORT_C" "Setup C->D"
check_result "Successfully connected to 127.0.0.1:$PORT_A" "Setup A->B"
check_result "Calculation result: 30" "Chain Add (A->B->C->D)"
check_result "Server time:" "Local GetTime"
check_result "Server response: Store mykey successfully" "Local Store"
check_result "Read result: demo_value" "Local Read"

if [ $FAILED -eq 0 ]; then
    echo "------------------------------------------------"
    echo "ALL 4-NODE TESTS PASSED SUCCESSFULLY!"
    echo "------------------------------------------------"
else
    echo "------------------------------------------------"
    echo "SOME TESTS FAILED. Execution Log Snippet:"
    grep "result\|failed\|Error" $TEST_LOG
    echo "------------------------------------------------"
fi

# 6. Cleanup
echo "Shutting down background nodes..."
kill $PID_B $PID_C $PID_D 2>/dev/null

# Clean up binary and all log files
rm $BINARY_NAME $TEST_LOG nodeB.log nodeC.log nodeD.log
echo "Cleanup complete. Workspace is tidy."
echo "Done."
