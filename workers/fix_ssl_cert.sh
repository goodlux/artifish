#!/bin/bash
# Script to fix SSL certificate issues on macOS for Python

echo "=== Fixing SSL Certificate Issues for Python ==="

# For macOS, run the certificate installer that comes with Python
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "Detected macOS, running certificate installer..."
    
    # Find the current Python installation
    PYTHON_PATH=$(which python)
    PYTHON_DIR=$(dirname $(dirname $PYTHON_PATH))
    
    echo "Python found at: $PYTHON_PATH"
    echo "Python directory: $PYTHON_DIR"
    
    # Check if we're in a virtual environment
    if [[ "$VIRTUAL_ENV" != "" ]]; then
        echo "Virtual environment detected: $VIRTUAL_ENV"
        echo "Will attempt to install certificates for your system Python..."
        
        # Try to find system Python's cert script
        SYSTEM_PYTHON_DIR="/Applications/Python 3.9" # Adjust version as needed
        
        if [ -d "$SYSTEM_PYTHON_DIR" ]; then
            echo "Found system Python directory: $SYSTEM_PYTHON_DIR"
            CERT_SCRIPT="$SYSTEM_PYTHON_DIR/Install Certificates.command"
            
            if [ -f "$CERT_SCRIPT" ]; then
                echo "Running certificate installer: $CERT_SCRIPT"
                bash "$CERT_SCRIPT"
            else
                echo "Certificate script not found at: $CERT_SCRIPT"
            fi
        else
            echo "System Python directory not found. Let's try a different approach..."
            
            # Alternative approach
            echo "Attempting to run Python's certifi certificate installation..."
            pip install --upgrade certifi
            python -m pip install --upgrade certifi
            
            echo "Creating a test script to find certificate path..."
            echo "import certifi; print(certifi.where())" > /tmp/certpath.py
            CERT_PATH=$(python /tmp/certpath.py)
            echo "Certificate path found: $CERT_PATH"
            
            echo "To use this in your code, you can add:"
            echo "import certifi"
            echo "session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl.create_default_context(cafile=certifi.where())))"
        fi
    else
        echo "No virtual environment detected, looking for certificate installer..."
        
        # Look for the certificate installer in standard locations
        POSSIBLE_PATHS=(
            "/Applications/Python 3.9/Install Certificates.command"
            "/Applications/Python 3.10/Install Certificates.command"
            "/Applications/Python 3.11/Install Certificates.command"
            "/Applications/Python 3.12/Install Certificates.command"
            "$PYTHON_DIR/Install Certificates.command"
        )
        
        CERT_SCRIPT=""
        for path in "${POSSIBLE_PATHS[@]}"; do
            if [ -f "$path" ]; then
                CERT_SCRIPT="$path"
                break
            fi
        done
        
        if [ "$CERT_SCRIPT" != "" ]; then
            echo "Running certificate installer: $CERT_SCRIPT"
            bash "$CERT_SCRIPT"
        else
            echo "Certificate installer not found. Let's try a different approach..."
            
            # Alternative approach
            echo "Attempting to run Python's certifi certificate installation..."
            pip install --upgrade certifi
            python -m pip install --upgrade certifi
            
            echo "Creating a test script to find certificate path..."
            echo "import certifi; print(certifi.where())" > /tmp/certpath.py
            CERT_PATH=$(python /tmp/certpath.py)
            echo "Certificate path found: $CERT_PATH"
            
            echo "To use this in your code, you can add:"
            echo "import certifi"
            echo "session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl.create_default_context(cafile=certifi.where())))"
        fi
    fi
else
    echo "Not on macOS, installing certifi package..."
    pip install --upgrade certifi
    python -m pip install --upgrade certifi
fi

echo ""
echo "=== SSL Certificate Fix Attempted ==="
echo "If the issue persists, you can modify the network_traversal_db_queue.py script to disable SSL verification temporarily."
echo "Look for the aiohttp.ClientSession() creation and add:"
echo "  connector=aiohttp.TCPConnector(verify_ssl=False)"
echo ""
echo "For example:"
echo "  self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(verify_ssl=False))"
echo ""
echo "SECURITY WARNING: Disabling SSL verification is not recommended for production environments!"
echo "This should only be used temporarily for debugging."
