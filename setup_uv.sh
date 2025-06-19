#!/bin/bash
# Script to set up the project using uv

echo "=== Setting up Artifish project with uv ==="

# Remove existing virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf .venv
fi

# Create virtual environment with uv
echo "Creating virtual environment with uv..."
uv venv

# Install dependencies
echo "Installing dependencies..."
uv pip install -e ".[dev]"

echo "=== Setup complete! ==="
echo "To activate the environment, run: source .venv/bin/activate"
echo "Available commands:"
echo "  uv run artifish            # Run main agent"
echo "  uv run pytest              # Run tests"
echo "  uv run black .             # Format code"
echo "  uv run mypy artifish/      # Type check"