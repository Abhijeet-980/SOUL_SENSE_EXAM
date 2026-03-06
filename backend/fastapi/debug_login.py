
import requests
import json
import sys

BASE_URL = "http://127.0.0.1:8000/api/v1"

def test_login_success():
    print(f"Testing login at {BASE_URL}...")
    headers = {"Content-Type": "application/json"}
    
    # Session ID for captcha (mock)
    session_id = "debug-session"
    
    # 1. Get captcha first (if needed, but mock might skip if we don't send one? 
    # Actually mock auth service requires it? Let's check auth.py logic.
    # It validates captcha if provided. 
    
    # Let's try to login with valid credentials
    payload = {
        "identifier": "admin@example.com",
        "password": "password", # Mock password doesn't matter much in mock mode?
        # Wait, the mock service uses a hash. let's see. 
        # API requires captcha_code if not disabled?
        # Let's generate a captcha first to be sure
    }
    
    # Generate Captcha
    try:
        resp = requests.get(f"{BASE_URL}/auth/captcha")
        if resp.status_code == 200:
            data = resp.json()
            captcha_code = data['captcha_code']
            session_id = data['session_id']
            print(f"Captcha generated: {captcha_code}, Session: {session_id}")
            payload['captcha_input'] = captcha_code
            payload['session_id'] = session_id
        else:
            print(f"Failed to generate captcha: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"Captcha generation failed: {e}")

    # Login
    try:
        response = requests.post(f"{BASE_URL}/auth/login", json=payload, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            if 'username' in data and 'email' in data:
                print("✅ SUCCESS: Username and Email found in response!")
                print(f"User: {data['username']}, Email: {data['email']}")
            else:
                print("❌ OUTDATED RESPONSE: 'username' or 'email' missing from Token response.")
        else:
             print("❌ Login Failed.")

    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    test_login_success()
