#!/bin/bash
# Setup script for Artifish project

# Create virtual environment
echo "Creating Python virtual environment..."
python3 -m venv .venv

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Install dependencies with UV if available, otherwise use pip
if command -v uv &> /dev/null; then
    echo "Installing dependencies with UV..."
    uv pip install -r requirements.txt
else
    echo "UV not found, installing dependencies with pip..."
    pip install -r requirements.txt
fi

# Create .env file from example if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from example..."
    cp .env.example .env
    echo "Please edit the .env file with your configuration details."
fi

echo "Setup complete! Virtual environment is activated."
echo "To activate the environment in the future, run: source .venv/bin/activate"
