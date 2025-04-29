"""
Quick script to modify the Bluesky client to use certifi certificates.
This is a proper fix for SSL certificate issues without disabling verification.
"""

import re

script_path = 'workers/network_traversal_db_queue.py'

# Read the file content
with open(script_path, 'r') as file:
    content = file.read()

# Check if we need to add the certifi import
if 'import certifi' not in content:
    content = re.sub(
        r'import aiohttp',
        'import aiohttp\nimport ssl\nimport certifi',
        content
    )

# Modify the session initialization to use certifi
certifi_session_init = """        if self.session is None:
            # Use certifi for certificate verification
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            self.session = aiohttp.ClientSession(connector=connector)
            logger.info("Using certifi for SSL certificate verification")"""

if 'certifi.where()' not in content:
    content = re.sub(
        r'        if self.session is None:\s+self\.session = aiohttp\.ClientSession\(\)',
        certifi_session_init,
        content
    )

# Write the modified content back to the file
with open(script_path, 'w') as file:
    file.write(content)

print("Successfully updated the script to use certifi for SSL certificate validation")
print("Next steps:")
print("1. Make sure you have certifi installed: pip install --upgrade certifi")
print("2. Run the network traversal script as usual")
