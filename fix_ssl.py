"""
Script to fix SSL certificate issues for the Bluesky crawler.

This script modifies the network_traversal_db_queue.py file to disable SSL verification
for debugging purposes only. In production, you should properly install SSL certificates.

How to use:
1. Run this script: python fix_ssl.py
2. Then run your network traversal script with the --no-verify-ssl flag:
   python -m workers.network_traversal_db_queue --no-verify-ssl
"""

import os
import re

# Path to the network traversal script
script_path = 'workers/network_traversal_db_queue.py'

# Read the file content
with open(script_path, 'r') as file:
    content = file.read()

# Add imports
if 'import ssl' not in content:
    content = re.sub(
        r'import aiohttp',
        'import aiohttp\nimport ssl\nimport certifi',
        content
    )

# Modify the BlueskyAPIClient class
content = re.sub(
    r'def __init__\(self, pds_host="bsky\.social", username=None, password=None\):',
    'def __init__(self, pds_host="bsky.social", username=None, password=None, verify_ssl=True):',
    content
)

# Add verify_ssl parameter
content = re.sub(
    r'self\.token_expires_in = 3600  # Default token lifetime in seconds \(1 hour\)',
    'self.token_expires_in = 3600  # Default token lifetime in seconds (1 hour)\n        self.verify_ssl = verify_ssl',
    content
)

# Modify the session initialization
session_init = """        if self.session is None:
            # Configure SSL verification based on settings
            if not self.verify_ssl:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                self.session = aiohttp.ClientSession(connector=connector)
                logger.warning("SSL verification disabled. This is insecure and should only be used for debugging!")
            else:
                try:
                    # Try to use certifi for certificate verification
                    ssl_context = ssl.create_default_context(cafile=certifi.where())
                    connector = aiohttp.TCPConnector(ssl=ssl_context)
                    self.session = aiohttp.ClientSession(connector=connector)
                    logger.info("Using certifi for SSL certificate verification")
                except Exception as e:
                    logger.warning(f"Error setting up certifi SSL context: {e}")
                    self.session = aiohttp.ClientSession()
                    logger.info("Using default SSL context")"""

content = re.sub(
    r'        if self.session is None:\s+self\.session = aiohttp\.ClientSession\(\)',
    session_init,
    content
)

# Modify the NetworkTraversal class
content = re.sub(
    r'def __init__\(self, supabase