#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

# trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1

for i in $(seq 1 $runs); do
    echo '***' TESTING TRIAL $i >> results.txt
    timeout -k 2s 900s go test -race -run 3A &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i >> results.txt
        exit 1
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS >> results.txt
