#!/bin/bash
set -e
cd /workspace/repo

TEST_CMD=$(python3 -c "
import json
print(json.load(open('/workspace/test_metadata.json'))['test_command'])
" 2>/dev/null)

if [ -z "$TEST_CMD" ]; then
    echo "ERROR: No test_command found in test_metadata.json"
    exit 1
fi

echo "Running: $TEST_CMD"
eval "$TEST_CMD"
