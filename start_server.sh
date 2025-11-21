#!/bin/bash

# Start all services: Embedding server and FastAPI
# Usage: ./start_server.sh

# Kill any existing processes on port 9000
echo "🧹 Cleaning up port 9000..."
lsof -ti:9000 | xargs kill -9 2>/dev/null || true
sleep 1

echo "🚀 Starting GPU Embedding server..."
python3 embedding_server.py &
EMBEDDING_PID=$!
sleep 5  # Wait for embedding server to start (model loading takes time)

echo "🚀 Starting FastAPI server..."
echo "✅ FastAPI running at: http://localhost:8000"
echo "✅ Embedding server running at: http://localhost:9000"
uvicorn server:app --host 0.0.0.0 --port 8000 --workers 4

# Cleanup on exit
kill $EMBEDDING_PID 2>/dev/null || true

