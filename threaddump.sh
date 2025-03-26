#!/bin/bash
# Use bash for better syntax and error handling (optional, sh works too)

# Check number of arguments
if [ $# -ne 3 ]; then
    echo "Usage: $0 PID Interval Count"
    exit 1
fi

PID=$1
INTERVAL=$2
COUNT=$3

# Validate PID exists
if ! ps -p "$PID" > /dev/null 2>&1; then
    echo "Error: PID $PID does not exist or is not running."
    exit 1
fi

# Check if jstack is available
if ! command -v jstack > /dev/null 2>&1; then
    echo "Error: jstack not found in PATH. Ensure JDK is installed and accessible."
    exit 1
fi

# Timestamp output files to avoid overwriting (optional)
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
TOP_OUT="top_${TIMESTAMP}.out"
JSTACK_OUT="jstack_${TIMESTAMP}.out"

# Start top in background
echo "Starting top for PID $PID with interval $INTERVAL seconds, $COUNT iterations..."
top -bH -d "$INTERVAL" -n "$COUNT" -p "$PID" >> "$TOP_OUT" 2>&1 &
TOP_PID=$!  # Capture top's PID for later synchronization

# Capture thread dumps
for i in $(seq 1 "$COUNT"); do
    echo "Capturing stack trace $i of $COUNT" >> "$JSTACK_OUT"
    jstack -l "$PID" >> "$JSTACK_OUT" 2>&1
    if [ $? -ne 0 ]; then
        echo "Warning: jstack failed for PID $PID on iteration $i" >> "$JSTACK_OUT"
    fi
    echo "--------------------" >> "$JSTACK_OUT"
    # Sleep only if not the last iteration
    [ "$i" -lt "$COUNT" ] && sleep "$INTERVAL"
done

# Wait for top to finish
wait "$TOP_PID"
echo "Done. Output saved to $TOP_OUT and $JSTACK_OUT"
