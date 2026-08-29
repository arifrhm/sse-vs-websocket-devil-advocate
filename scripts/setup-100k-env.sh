#!/bin/bash

echo "=== 100K Environment Setup ==="

# Set file descriptor limits
ulimit -n 200000 2>/dev/null || ulimit -n 100000 2>/dev/null || ulimit -n 65536 2>/dev/null
echo "Current ulimit -n: $(ulimit -n)"

# Setup loopback IP aliases on macOS if needed (127.0.0.2 - 127.0.0.8)
if [[ "$OSTYPE" == "darwin"* ]]; then
  echo "Setting up loopback IP aliases on macOS..."
  for i in {2..8}; do
    if ! ifconfig lo0 | grep -q "127.0.0.$i"; then
      echo "Adding alias 127.0.0.$i..."
      sudo ifconfig lo0 alias 127.0.0.$i up 2>/dev/null || true
    else
      echo "Alias 127.0.0.$i already exists."
    fi
  done
elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
  echo "Linux environment detected. Ensuring kernel socket limits..."
  sudo sysctl -w net.core.somaxconn=65535 2>/dev/null || true
  sudo sysctl -w net.ipv4.ip_local_port_range="1024 65535" 2>/dev/null || true
fi

echo "Environment setup complete."
