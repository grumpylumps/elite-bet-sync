#!/usr/bin/env bash
# Start the sync server binding to all interfaces (0.0.0.0)
PORT=${1:-8081}
export HOST=0.0.0.0
if [ ! -d "node_modules" ]; then
  echo "Installing dependencies..."
  npm install --no-audit --no-fund
fi
node src/index.js &
PID=$!
sleep 1
# Wait for health
for i in {1..10}; do
  if curl -s -f http://localhost:${PORT}/health >/dev/null 2>&1; then
    echo "Sync server is up on http://0.0.0.0:${PORT} (PID: ${PID})"
    exit 0
  fi
  sleep 1
done
echo "Server did not start within timeout"
kill ${PID}
exit 1
