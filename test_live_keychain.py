import os
import logging
import sys

# Ensure the root directory is in sys.path so app can be imported
sys.path.insert(0, os.path.abspath(os.curdir))

# Enable the feature flag for this session
os.environ["SOULSENSE_FF_MACOS_KEYCHAIN_INTEGRATION"] = "true"

from app.auth.crypto import EncryptionManager

# Configure logging to see the results
logging.basicConfig(level=logging.INFO)

print("--- Starting Live Keychain Test ---")
# 1. Trigger key retrieval/generation
key = EncryptionManager._get_key()
# Key might be bytes or string depending on state, handle safely
if isinstance(key, bytes):
    key_preview = key[:10].decode()
else:
    key_preview = str(key)[:10]
    
print(f"Master Key accessed: {key_preview}...")

# 2. Verify encryption works with this key
test_data = "Secret message"
encrypted = EncryptionManager.encrypt(test_data)
decrypted = EncryptionManager.decrypt(encrypted)

if test_data == decrypted:
    print("SUCCESS: Encryption/Decryption verified with Keychain-backed key.")
else:
    print("FAILURE: Decryption mismatch.")
