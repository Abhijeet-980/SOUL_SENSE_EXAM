import sys
import os
from unittest.mock import MagicMock

# Add the backend directory to sys.path
sys.path.append(os.path.abspath("backend/fastapi"))

from api.utils.network import get_real_ip

def test_hardened_get_real_ip():
    # Mock Request
    request = MagicMock()
    
    # 1. Setup Mock Settings with a specific Trusted Proxy
    from api.config import get_settings_instance
    settings = get_settings_instance()
    settings.TRUSTED_PROXIES = ["10.0.0.1"]

    # Case A: Request from UNTRUSTED IP trying to spoof
    request.client.host = "203.0.113.5"
    request.headers = {"X-Forwarded-For": "1.1.1.1"}
    print(f"Case A (Spoof Attempt from 203.0.113.5): Result={get_real_ip(request)} (Expected=203.0.113.5)")
    assert get_real_ip(request) == "203.0.113.5"

    # Case B: Request from TRUSTED Proxy with real IP
    request.client.host = "10.0.0.1"
    request.headers = {"X-Forwarded-For": "1.1.1.1"}
    print(f"Case B (Trusted Proxy 10.0.0.1): Result={get_real_ip(request)} (Expected=1.1.1.1)")
    assert get_real_ip(request) == "1.1.1.1"

    # Case C: Chained Proxies from Trusted Proxy
    request.client.host = "10.0.0.1"
    request.headers = {"X-Forwarded-For": "1.2.3.4, 10.0.0.2"}
    print(f"Case C (Chained via Trusted Proxy): Result={get_real_ip(request)} (Expected=1.2.3.4)")
    assert get_real_ip(request) == "1.2.3.4"

    print("--- Hardening tests passed! ---")

if __name__ == "__main__":
    test_hardened_get_real_ip()
