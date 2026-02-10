# Mock Authentication Mode

## Overview

Mock Authentication Mode is a testing and development feature that allows the application to run without requiring real user credentials or database operations. This is particularly useful for:

- **Automated Testing**: Run tests without setting up a full database
- **Development**: Quickly test features without authentication overhead
- **CI/CD Pipelines**: Simplify continuous integration testing
- **Demos**: Showcase features without exposing real user data

## Features

✅ **Simulated Login Flow**: Authenticate with predefined test users  
✅ **No Database Required**: All operations work in-memory  
✅ **2FA Support**: Test two-factor authentication flows  
✅ **Password Reset**: Simulate password reset with mock OTP codes  
✅ **Token Management**: Full JWT token lifecycle (access + refresh)  
✅ **Same API Interface**: Drop-in replacement for real auth service  

## Enabling Mock Mode

### Backend Configuration

Set the `MOCK_AUTH_MODE` environment variable to `true`:

```bash
# In .env file
MOCK_AUTH_MODE=true
```

Or set it programmatically:

```bash
# Windows (PowerShell)
$env:MOCK_AUTH_MODE="true"

# Windows (CMD)
set MOCK_AUTH_MODE=true

# Linux/Mac
export MOCK_AUTH_MODE=true
```

### Frontend Configuration

The frontend automatically works with mock mode when the backend is configured. No additional changes needed.

## Mock Test Users

The following test users are available in mock mode:

### Standard User
- **Email**: `test@example.com`
- **Username**: `testuser`
- **Password**: Any password (not validated in mock mode)
- **2FA**: Disabled
- **OTP Code**: `123456`

### Admin User
- **Email**: `admin@example.com`
- **Username**: `admin`
- **Password**: Any password
- **2FA**: Disabled
- **OTP Code**: `654321`

### 2FA User
- **Email**: `2fa@example.com`
- **Username**: `twofa`
- **Password**: Any password
- **2FA**: Enabled
- **OTP Code**: `999999`

### 2FA Setup Code
- **Code**: `888888` (used when enabling 2FA)

## Usage Examples

### Basic Login

```python
# Any password works in mock mode
POST /api/v1/auth/login
{
    "username": "test@example.com",
    "password": "anything"
}

# Response
{
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
    "token_type": "bearer",
    "refresh_token": "..."
}
```

### 2FA Login Flow

```python
# Step 1: Initial login (returns pre-auth token)
POST /api/v1/auth/login
{
    "username": "2fa@example.com",
    "password": "anything"
}

# Response (202 Accepted)
{
    "pre_auth_token": "eyJ0eXAiOiJKV1QiLCJhbGc..."
}

# Step 2: Verify 2FA code
POST /api/v1/auth/login/2fa
{
    "pre_auth_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
    "code": "999999"
}

# Response
{
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGc...",
    "token_type": "bearer",
    "refresh_token": "..."
}
```

### Password Reset Flow

```python
# Step 1: Initiate password reset
POST /api/v1/auth/password-reset/initiate
{
    "email": "test@example.com"
}

# Check logs for OTP code (123456 for test@example.com)

# Step 2: Complete password reset
POST /api/v1/auth/password-reset/complete
{
    "email": "test@example.com",
    "otp_code": "123456",
    "new_password": "new_password_123"
}
```

### Enable 2FA

```python
# Step 1: Initiate 2FA setup (requires authentication)
POST /api/v1/auth/2fa/setup/initiate
Headers: Authorization: Bearer <access_token>

# Check logs for OTP code (always 888888 for setup)

# Step 2: Enable 2FA
POST /api/v1/auth/2fa/enable
Headers: Authorization: Bearer <access_token>
{
    "code": "888888"
}
```

## Testing with Mock Mode

### Running Tests

```bash
# Set environment variable
$env:MOCK_AUTH_MODE="true"

# Run tests
pytest tests/test_mock_auth.py -v
```

### Example Test

```python
import os
os.environ["MOCK_AUTH_MODE"] = "true"

from backend.fastapi.api.services.mock_auth_service import MockAuthService

def test_login():
    auth_service = MockAuthService()
    
    # Authenticate user
    user = auth_service.authenticate_user(
        "test@example.com",
        "any_password"
    )
    
    assert user is not None
    assert user.email == "test@example.com"
    
    # Create access token
    token = auth_service.create_access_token(
        data={"sub": user.username}
    )
    
    assert token is not None
```

## Logging

Mock authentication includes detailed logging with 🎭 emoji prefix:

```
🎭 Mock Authentication Service initialized
🎭 Mock authentication attempt for: test@example.com
✅ Mock authentication successful for: test@example.com
🎭 Created mock access token for user_id: 1
🎭 Mock 2FA initiated for 2fa@example.com. OTP: 999999
```

## Security Considerations

⚠️ **IMPORTANT**: Mock authentication mode should **NEVER** be enabled in production!

- **Development Only**: Use only in development and testing environments
- **No Real Security**: Passwords are not validated, any value works
- **Predictable Tokens**: OTP codes are fixed and predictable
- **In-Memory Storage**: All data is lost when the service restarts

### Production Safety

The configuration system prevents accidental production use:

```python
# In production settings
class ProductionSettings(BaseAppSettings):
    app_env: str = "production"
    mock_auth_mode: bool = False  # Always False in production
```

## Troubleshooting

### Mock mode not working

1. **Check environment variable**:
   ```bash
   echo $env:MOCK_AUTH_MODE  # Windows PowerShell
   ```

2. **Verify configuration**:
   ```python
   from backend.fastapi.api.config import get_settings
   settings = get_settings()
   print(f"Mock mode: {settings.mock_auth_mode}")
   ```

3. **Check logs**: Look for 🎭 emoji in logs

### Tests failing

1. **Ensure mock mode is enabled** before importing modules
2. **Use correct OTP codes** from the documentation
3. **Check that test users exist** in MOCK_USERS dictionary

## Implementation Details

### Architecture

```
┌─────────────────────────────────────┐
│         Auth Router                 │
│  (Handles HTTP requests)            │
└────────────┬────────────────────────┘
             │
             ▼
┌─────────────────────────────────────┐
│    get_auth_service()               │
│  (Dependency injection)             │
└────────┬───────────────────┬────────┘
         │                   │
         ▼                   ▼
┌────────────────┐  ┌────────────────┐
│  AuthService   │  │ MockAuthService│
│  (Real auth)   │  │  (Mock auth)   │
└────────────────┘  └────────────────┘
```

### Service Selection

The `get_auth_service()` dependency function automatically selects the appropriate service:

```python
def get_auth_service(db: Session = Depends(get_db)) -> AuthService:
    if settings.mock_auth_mode:
        return MockAuthService(db)
    return AuthService(db)
```

### Mock Data Storage

Mock users and tokens are stored in module-level dictionaries:

```python
MOCK_USERS = {
    "test@example.com": {...},
    "admin@example.com": {...},
    "2fa@example.com": {...}
}

MOCK_OTP_CODES = {
    "test@example.com": "123456",
    "admin@example.com": "654321",
    "2fa@example.com": "999999"
}

MOCK_REFRESH_TOKENS = {}  # Populated at runtime
```

## API Compatibility

Mock authentication maintains 100% API compatibility with real authentication:

| Feature | Real Auth | Mock Auth |
|---------|-----------|-----------|
| Login | ✅ | ✅ |
| Register | ✅ | ✅ |
| 2FA | ✅ | ✅ |
| Password Reset | ✅ | ✅ |
| Token Refresh | ✅ | ✅ |
| Token Revocation | ✅ | ✅ |
| JWT Tokens | ✅ | ✅ |

## Future Enhancements

Potential improvements for mock authentication:

- [ ] Configurable mock users via environment variables
- [ ] Mock user registration persistence (in-memory)
- [ ] Simulated rate limiting
- [ ] Configurable token expiration times
- [ ] Mock email service integration
- [ ] Performance metrics and logging

## Related Files

- **Service**: `backend/fastapi/api/services/mock_auth_service.py`
- **Config**: `backend/fastapi/api/config.py`
- **Router**: `backend/fastapi/api/routers/auth.py`
- **Tests**: `tests/test_mock_auth.py`
- **Example Env**: `.env.test.example`

## Support

For issues or questions about mock authentication:

1. Check this documentation
2. Review test files for examples
3. Check application logs for 🎭 indicators
4. Verify environment configuration

---

**Last Updated**: 2026-02-10  
**Version**: 1.0.0
