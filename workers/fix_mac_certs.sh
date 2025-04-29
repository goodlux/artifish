#!/bin/bash
# Script to fix SSL certificate issues on macOS for Python

echo "=== Installing and verifying Python SSL certificates on macOS ==="

# Install certifi
echo "Installing certifi package..."
pip install --upgrade certifi

# Find Python installation paths
PYTHON_PATH=$(which python)
PYTHON_BIN=$(dirname "$PYTHON_PATH")
PYTHON_ROOT=$(dirname "$PYTHON_BIN")

echo "Python path: $PYTHON_PATH"
echo "Python root: $PYTHON_ROOT"

# Check for macOS certificate installer
CERT_PATHS=(
  "/Applications/Python 3.9/Install Certificates.command"
  "/Applications/Python 3.10/Install Certificates.command"
  "/Applications/Python 3.11/Install Certificates.command"
  "/Applications/Python 3.12/Install Certificates.command"
  "$PYTHON_ROOT/Install Certificates.command"
)

CERT_INSTALLER=""
for path in "${CERT_PATHS[@]}"; do
  if [ -f "$path" ]; then
    CERT_INSTALLER="$path"
    break
  fi
done

if [ -n "$CERT_INSTALLER" ]; then
  echo "Found certificate installer: $CERT_INSTALLER"
  echo "Running the certificate installer..."
  bash "$CERT_INSTALLER"
else
  echo "Certificate installer not found. Creating a helper script..."
  
  # Create a small Python script to test SSL connections
  cat > test_ssl.py << EOF
import ssl
import certifi
import requests

# Print certificate information
print(f"Using certificate file: {certifi.where()}")

# Test connection to bsky.social
print("Testing connection to bsky.social...")
try:
    response = requests.get("https://bsky.social/xrpc/.well-known/did-configuration", timeout=10)
    print(f"Connection successful! Status code: {response.status_code}")
    print(f"Response: {response.text[:100]}...")
except Exception as e:
    print(f"Connection failed: {str(e)}")

# Additional help
print("\nTo fix certificate issues in your code:")
print("1. Use requests with certifi:")
print("   requests.get('https://example.com', verify=certifi.where())")
print("2. Use aiohttp with certifi:")
print("   ssl_context = ssl.create_default_context(cafile=certifi.where())")
print("   connector = aiohttp.TCPConnector(ssl=ssl_context)")
print("   session = aiohttp.ClientSession(connector=connector)")
EOF

  # Install requests if needed
  pip install requests
  
  # Run the test script
  echo "Running SSL test script..."
  python test_ssl.py
fi

echo "=== Certificate installation complete ==="
echo "Run the use_certifi.py script to update your network traversal code:"
echo "python workers/use_certifi.py"
