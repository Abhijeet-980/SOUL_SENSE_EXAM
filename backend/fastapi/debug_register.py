
import requests
import json
import sys

BASE_URL = "http://127.0.0.1:8000/api/v1"

def test_register():
    print(f"Testing registration at {BASE_URL}...")
    headers = {"Content-Type": "application/json"}
    
    # Randomize email to allow repeated runs
    import random
    import string
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    email = f"test_{random_str}@example.com"
    username = f"user_{random_str}"
    
    payload = {
        "email": email,
        "username": username,
        "password": "Password123!",
        "first_name": "Test",
        "last_name": "User",
        "age": 25,
        "gender": "Other"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/auth/register", json=payload, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")
        
        if response.status_code == 200:
            print("✅ SUCCESS: Registration successful!")
        else:
             print("❌ Registration Failed.")

    except Exception as e:
        print(f"Request failed: {e}")

if __name__ == "__main__":
    test_register()
