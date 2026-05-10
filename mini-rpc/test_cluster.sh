#!/bin/bash

# Configuration
BINARY_NAME="node_app"
PORT_A=1234
PORT_B=1235
PORT_C=1236

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

# 3. Start Node C and Node B in the background
echo "Starting Node C on port $PORT_C..."
./$BINARY_NAME -port $PORT_C > nodeC.log 2>&1 &
PID_C=$!

echo "Starting Node B on port $PORT_B..."
./$BINARY_NAME -port $PORT_B > nodeB.log 2>&1 &
PID_B=$!

# Give nodes more time to initialize (2 seconds is safer for RPC registration)
echo "Waiting for nodes to initialize..."
sleep 2

# 4. Start Node A and Run Automated Tests
echo "------------------------------------------------"
echo "Running Automated Integration Tests..."
echo "Node A -> Node B (Port $PORT_B)"
echo "------------------------------------------------"

# Create a temporary file for test output
TEST_LOG="test_execution.log"

# Pipe commands into Node A
# Using 127.0.0.1 for better stability across different network stacks
./$BINARY_NAME -port $PORT_A <<EOF > $TEST_LOG 2>&1
dial 127.0.0.1:$PORT_B
getTime
add 50 25
store auto_key hello_from_A
read auto_key
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

check_result "Successfully connected to 127.0.0.1:$PORT_B" "Connection (Dial)"
check_result "Server time:" "GetTime RPC"
check_result "Calculation result: 75" "Add RPC (50+25)"
check_result "Server response: Store auto_key successfully" "Store RPC"
check_result "Read result: hello_from_A" "Read RPC"

if [ $FAILED -eq 0 ]; then
    echo "------------------------------------------------"
    echo "ALL TESTS PASSED SUCCESSFULLY!"
    echo "------------------------------------------------"
else
    echo "------------------------------------------------"
    echo "SOME TESTS FAILED. Execution Log Snippet:"
    cat $TEST_LOG
    echo "------------------------------------------------"
fi

# 6. Cleanup
echo "Shutting down background nodes..."
kill $PID_B $PID_C 2>/dev/null

# Clean up binary and all log files
rm $BINARY_NAME $TEST_LOG nodeB.log nodeC.log
echo "Cleanup complete. Workspace is tidy."
echo "Done."
