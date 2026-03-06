import requests
import time

BASE_URL = "http://localhost:8000/api/v1/auth"

def test_login_rate_limit():
    print("Testing Login Rate Limiting...")
    # Attempt login multiple times to trigger rate limit (Max 5 in 15 mins)
    for i in range(7):
        payload = {
            "username": "testuser_nonexistent",
            "password": "wrongpassword"
        }
        # auth/login expects form data
        data = {
            "username": payload["username"],
            "password": payload["password"]
        }
        response = requests.post(f"{BASE_URL}/login", data=data)
        print(f"Attempt {i+1}: {response.status_code} - {response.text[:100]}")
        if response.status_code == 429:
            print("SUCCESS: Rate limit triggered!")
            return True
        time.sleep(0.5)
    return False

def test_registration_enumeration():
    print("\nTesting Registration Enumeration...")
    # Attempt to register with a known existing email/username
    # Note: We assume 'testuser' exists from previous tests or seed
    payload = {
        "username": "testuser",
        "email": "test@example.com",
        "password": "StrongPassword123!",
        "first_name": "Test",
        "last_name": "User",
        "age": 25,
        "gender": "Male"
    }
    response = requests.post(f"{BASE_URL}/register", json=payload)
    print(f"Response: {response.status_code} - {response.json()}")
    if response.status_code == 200:
        print("SUCCESS: Registration returned 200/Generic even for existing user!")
        return True
    return False

if __name__ == "__main__":
    test_login_rate_limit()
    test_registration_enumeration()
