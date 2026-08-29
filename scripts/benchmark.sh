#!/bin/bash

# Configuration
CONNECTIONS=100000
DURATION=30
WRITE_INTERVAL=5000
PORT=8080

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_FILE="$PROJECT_DIR/benchmark-results.txt"

# Run 100K OS environment setup
bash "$PROJECT_DIR/scripts/setup-100k-env.sh"

echo "=== Devil's Advocate Benchmark Suite ===" > "$RESULTS_FILE"
echo "Connections: $CONNECTIONS" >> "$RESULTS_FILE"
echo "Duration: $DURATION seconds" >> "$RESULTS_FILE"
echo "Write Interval: $WRITE_INTERVAL ms" >> "$RESULTS_FILE"
echo "Date: $(date)" >> "$RESULTS_FILE"
echo "========================================" >> "$RESULTS_FILE"

# Helper function to run benchmark for a specific language and server type
run_bench() {
  local lang=$1
  local server=$2 # ws, sse1, sse2
  local proto=$3  # ws, sse-http1, sse-http2
  local bidi=$4   # true, false

  echo "----------------------------------------"
  echo "Running $lang - $server ($proto) | Bidirectional: $bidi"
  echo "----------------------------------------"
  echo "" >> "$RESULTS_FILE"
  echo "### Lang: $lang | Mode: $server ($proto) | Bidirectional: $bidi" >> "$RESULTS_FILE"

  # Start the server
  local server_pid
  if [ "$lang" == "typescript" ]; then
    node "$PROJECT_DIR/typescript/dist/$server-server.js" &
    server_pid=$!
  elif [ "$lang" == "go" ]; then
    "$PROJECT_DIR/go/go-server" --server "$server" --port "$PORT" &
    server_pid=$!
  elif [ "$lang" == "rust" ]; then
    "$PROJECT_DIR/rust/target/release/sse-vs-websocket-devil-advocate-rust" --server "$server" --port "$PORT" &
    server_pid=$!
  fi

  # Give server time to boot
  sleep 3

  # Run load tester
  node "$PROJECT_DIR/clients/load-tester.js" \
    --protocol="$proto" \
    --connections="$CONNECTIONS" \
    --port="$PORT" \
    --bidirectional="$bidi" \
    --writeInterval="$WRITE_INTERVAL" \
    --duration="$DURATION" >> "$RESULTS_FILE"

  # Kill the server
  echo "Cleaning up server PID $server_pid..."
  kill -9 $server_pid 2>/dev/null
  sleep 2
  
  # Ensure no processes are left hanging on the port
  local left_pids=$(lsof -t -i :$PORT)
  if [ ! -z "$left_pids" ]; then
    echo "Killing leftover processes on port $PORT: $left_pids"
    kill -9 $left_pids 2>/dev/null
    sleep 1
  fi
}

# Run TypeScript benchmarks
echo "=== Starting TypeScript Benchmarks ==="
run_bench "typescript" "ws" "ws" "false"
run_bench "typescript" "sse-http1" "sse-http1" "false"
run_bench "typescript" "sse-http2" "sse-http2" "false"

run_bench "typescript" "ws" "ws" "true"
run_bench "typescript" "sse-http1" "sse-http1" "true"
run_bench "typescript" "sse-http2" "sse-http2" "true"

# Run Go benchmarks
echo "=== Starting Go Benchmarks ==="
run_bench "go" "ws" "ws" "false"
run_bench "go" "sse1" "sse-http1" "false"
run_bench "go" "sse2" "sse-http2" "false"

run_bench "go" "ws" "ws" "true"
run_bench "go" "sse1" "sse-http1" "true"
run_bench "go" "sse2" "sse-http2" "true"

# Run Rust benchmarks
echo "=== Starting Rust Benchmarks ==="
run_bench "rust" "ws" "ws" "false"
run_bench "rust" "sse1" "sse-http1" "false"
run_bench "rust" "sse2" "sse-http2" "false"

run_bench "rust" "ws" "ws" "true"
run_bench "rust" "sse1" "sse-http1" "true"
run_bench "rust" "sse2" "sse-http2" "true"

echo "========================================"
echo "Benchmark Suite Complete! Results written to $RESULTS_FILE"
echo "========================================"
cat "$RESULTS_FILE"
