#!/bin/bash
# Script to rebuild the virtual environment for Artifish project

echo "=== Rebuilding Python virtual environment ==="

# Remove existing virtual environment if it exists
if [ -d ".venv" ]; then
    echo "Removing existing virtual environment..."
    rm -rf .venv
fi

# Use fixed requirements file
if [ -f "requirements.txt.fixed" ]; then
    echo "Using fixed requirements file..."
    mv requirements.txt.fixed requirements.txt
fi

# Create a new virtual environment
echo "Creating new Python virtual environment..."
python3 -m venv .venv

# Activate the virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate

# Verify Python is working correctly
echo "Checking Python version and encoding support..."
python -c "import sys; print(f'Python {sys.version}')"
python -c "import encodings; print('Encodings module found!')"

# Install pip-tools first for better dependency management
echo "Installing pip-tools..."
pip install --upgrade pip pip-tools

# Install requirements
echo "Installing requirements..."
pip install -r requirements.txt

echo "=== Virtual environment rebuild complete! ==="
echo "If you see no errors above, your environment should be fixed."
echo "To activate the environment, run: source .venv/bin/activate"
