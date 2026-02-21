# Quick Start: Mock Authentication Mode

This guide will help you quickly set up and test the mock authentication feature.

## 🚀 Quick Setup (5 minutes)

### Step 1: Enable Mock Mode

Create a `.env` file in the project root (or copy `.env.test.example`):

```bash
# Copy the example file
cp .env.test.example .env

# Or create manually with this content:
MOCK_AUTH_MODE=true
APP_ENV=development
```

### Step 2: Start the Backend

```bash
# Navigate to backend directory
cd backend/fastapi

# Install dependencies (if not already done)
pip install -r requirements.txt

# Start the server
uvicorn api.main:app --reload --port 8000
```

### Step 3: Verify Mock Mode is Active

Check the server logs for the 🎭 emoji:

```
🎭 Mock Authentication Service initialized
```

Or visit: `http://localhost:8000/api/v1/health`

### Step 4: Test Login

Use any of these test accounts:

**Standard User:**
- Email: `test@example.com`
- Password: `anything` (any password works!)

**Admin User:**
- Email: `admin@example.com`
- Password: `anything`

**2FA User:**
- Email: `2fa@example.com`
- Password: `anything`
- OTP Code: `999999`

## 🧪 Testing

### Run Mock Auth Tests

```bash
# Set mock mode
$env:MOCK_AUTH_MODE="true"  # Windows PowerShell
# export MOCK_AUTH_MODE=true  # Linux/Mac

# Run tests
pytest tests/test_mock_auth.py -v
```

### Manual API Testing

#### Login Request
```bash
curl -X POST http://localhost:8000/api/v1/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=test@example.com&password=anything"
```

#### Expected Response
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
  "token_type": "bearer",
  "refresh_token": "..."
}
```

## 📝 Test Scenarios

### Scenario 1: Basic Login
```python
# Login with test user
POST /api/v1/auth/login
Body: username=test@example.com&password=anything

# Expected: 200 OK with tokens
```

### Scenario 2: 2FA Flow
```python
# Step 1: Login with 2FA user
POST /api/v1/auth/login
Body: username=2fa@example.com&password=anything

# Expected: 202 Accepted with pre_auth_token

# Step 2: Verify 2FA
POST /api/v1/auth/login/2fa
Body: {
  "pre_auth_token": "<token_from_step_1>",
  "code": "999999"
}

# Expected: 200 OK with tokens
```

### Scenario 3: Password Reset
```python
# Step 1: Initiate reset
POST /api/v1/auth/password-reset/initiate
Body: {"email": "test@example.com"}

# Check logs for OTP: 123456

# Step 2: Complete reset
POST /api/v1/auth/password-reset/complete
Body: {
  "email": "test@example.com",
  "otp_code": "123456",
  "new_password": "new_pass"
}

# Expected: 200 OK
```

## 🎯 Common Use Cases

### Use Case 1: Frontend Development
Enable mock mode to develop frontend features without backend dependencies:

```typescript
// useAuth.tsx will automatically detect mock mode
const { login, isMockMode } = useAuth();

// Login works the same way
await login('test@example.com', 'anything', true);
```

### Use Case 2: Automated Testing
```python
import os
os.environ["MOCK_AUTH_MODE"] = "true"

from backend.fastapi.api.services.mock_auth_service import MockAuthService

def test_user_flow():
    auth = MockAuthService()
    user = auth.authenticate_user("test@example.com", "anything")
    assert user is not None
```

### Use Case 3: CI/CD Pipeline
```yaml
# .github/workflows/test.yml
env:
  MOCK_AUTH_MODE: true
  
steps:
  - name: Run tests
    run: pytest tests/
```

## 🔍 Troubleshooting

### Problem: Mock mode not working

**Solution 1**: Check environment variable
```bash
# Windows PowerShell
echo $env:MOCK_AUTH_MODE

# Linux/Mac
echo $MOCK_AUTH_MODE
```

**Solution 2**: Verify in code
```python
from backend.fastapi.api.config import get_settings
settings = get_settings()
print(f"Mock mode: {settings.mock_auth_mode}")
```

### Problem: Login fails with 401

**Possible causes:**
1. Mock mode not enabled
2. Using wrong test email (must be one of the predefined users)
3. Backend not running

**Solution:**
```bash
# Check backend is running
curl http://localhost:8000/api/v1/health

# Verify mock users are available
# Should see test@example.com, admin@example.com, 2fa@example.com
```

### Problem: 2FA code not working

**Solution:** Use the correct OTP codes:
- `test@example.com`: `123456`
- `admin@example.com`: `654321`
- `2fa@example.com`: `999999`
- 2FA Setup: `888888`

## 📚 Next Steps

1. **Read Full Documentation**: See `docs/MOCK_AUTH.md` for complete details
2. **Explore Test Users**: Try all predefined users and flows
3. **Run Test Suite**: Execute `pytest tests/test_mock_auth.py -v`
4. **Frontend Integration**: Add `<MockModeBanner />` to your app layout

## ⚠️ Important Reminders

- ✅ **DO** use mock mode for development and testing
- ✅ **DO** use any password (it's not validated)
- ✅ **DO** check logs for OTP codes (marked with 🎭)
- ❌ **DON'T** enable mock mode in production
- ❌ **DON'T** commit `.env` with mock mode enabled
- ❌ **DON'T** rely on mock mode for security testing

## 🆘 Getting Help

If you encounter issues:

1. Check the logs for 🎭 indicators
2. Verify environment variables are set
3. Review `docs/MOCK_AUTH.md` for detailed documentation
4. Check `tests/test_mock_auth.py` for working examples

## 📋 Checklist

Before reporting issues, verify:

- [ ] `MOCK_AUTH_MODE=true` is set in environment
- [ ] Backend server is running
- [ ] Using one of the predefined test users
- [ ] Checked server logs for errors
- [ ] Tried restarting the backend server

---

**Happy Testing! 🎭**
