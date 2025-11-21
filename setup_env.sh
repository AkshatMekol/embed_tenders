#!/bin/bash

# Setup script for embed_tenderss project
# Installs uv, creates virtual environment, and installs dependencies

set -e  # Exit on error

echo "🚀 Setting up environment..."

# Install uv if not already installed
if ! command -v uv &> /dev/null; then
    echo "📦 Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh

    # Add uv to PATH for current session
    export PATH="$HOME/.cargo/bin:$PATH"
else
    echo "✅ uv is already installed"
fi

# Ensure uv is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Create virtual environment using uv
echo "🔧 Creating virtual environment..."
uv venv

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source .venv/bin/activate

# Install dependencies from lock file
echo "📥 Installing dependencies from uv.lock..."
uv sync

echo "✅ Environment setup complete!"
echo ""
echo "To activate the environment in the future, run:"
echo "  source .venv/bin/activate"

