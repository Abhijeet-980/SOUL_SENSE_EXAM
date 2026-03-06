# Login Delay Feature - Detailed Implementation Roadmap

## Overview

**Goal:** Add progressive delay between failed login attempts to prevent brute-force attacks while providing clear feedback to legitimate users.

---

## Expected Behavior

| Failure Count | Delay Duration | User Experience |
|---------------|----------------|-----------------|
| 1-2 | 0 seconds | Immediate retry allowed |
| 3 | 5 seconds | Brief wait, countdown shown |
| 4 | 15 seconds | Short wait, countdown shown |
| 5 | 30 seconds | Moderate wait, countdown shown |
| 6+ | 60 seconds | Extended wait, account lockout warning |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        FRONTEND                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                 Login Form                           │    │
│  │  ┌──────────────────────────────────────────────┐   │    │
│  │  │  Email: [_______________]                     │   │    │
│  │  │  Password: [_______________]                  │   │    │
│  │  │                                               │   │    │
│  │  │  ┌─────────────────────────────────────────┐ │   │    │
│  │  │  │  ⏳ Please wait 15 seconds before       │ │   │    │
│  │  │  │     retrying... (countdown: 12s)        │ │   │    │
│  │  │  └─────────────────────────────────────────┘ │   │    │
│  │  │                                               │   │    │
│  │  │  [Login] (disabled during countdown)         │   │    │
│  │  └──────────────────────────────────────────────┘   │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        BACKEND                               │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  POST /api/v1/auth/login                             │    │
│  │                                                      │    │
│  │  Response (on failure):                              │    │
│  │  {                                                   │    │
│  │    "success": false,                                 │    │
│  │    "error": "Invalid credentials",                   │    │
│  │    "retry_after": 15,        ← seconds to wait      │    │
│  │    "attempts_remaining": 2    ← before lockout      │    │
│  │  }                                                   │    │
│  └─────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────┘
```

---

## Implementation Tasks

### Task 1: Backend - Implement Delay Logic

**File:** `backend/fastapi/api/routers/auth.py`

#### Requirements
- [ ] Track failed attempts per email/IP in `LoginAttempt` table
- [ ] Calculate delay based on failure count using exponential backoff
- [ ] Return `retry_after` seconds in API response
- [ ] Return `attempts_remaining` before lockout
- [ ] Enforce delay server-side (reject early requests)

#### Implementation Details

```python
# Delay calculation formula
def calculate_delay(attempt_count: int) -> int:
    """
    Progressive delay based on failed attempts.
    
    Attempts 1-2: 0 seconds
    Attempt 3: 5 seconds
    Attempt 4: 15 seconds  
    Attempt 5: 30 seconds
    Attempt 6+: 60 seconds
    """
    delays = [0, 0, 5, 15, 30, 60]
    index = min(attempt_count - 1, len(delays) - 1)
    return delays[max(0, index)]
```

#### API Response Schema Update

```python
class LoginErrorResponse(BaseModel):
    success: bool = False
    error: str
    retry_after: int = 0  # Seconds until next attempt allowed
    attempts_remaining: int  # Attempts before lockout
    locked_until: Optional[datetime] = None  # If account is locked
```

#### Acceptance Criteria
- [ ] Delay increases progressively with each failure
- [ ] Early retry requests return 429 Too Many Requests
- [ ] Response includes `retry_after` field
- [ ] Successful login resets failure count

---

### Task 2: Frontend - Show Countdown UI

**Files:** 
- `frontend-web/src/app/login/page.tsx`
- `frontend-web/src/components/auth/login-countdown.tsx`

#### Requirements
- [ ] Parse `retry_after` from API response
- [ ] Display countdown timer (updating every second)
- [ ] Disable login button during countdown
- [ ] Show warning message with attempts remaining
- [ ] Auto-enable form when countdown reaches 0

#### Component: LoginCountdown

```tsx
interface LoginCountdownProps {
  seconds: number;
  attemptsRemaining: number;
  onComplete: () => void;
}

// Usage in login form:
{retryAfter > 0 && (
  <LoginCountdown 
    seconds={retryAfter}
    attemptsRemaining={attemptsRemaining}
    onComplete={() => setRetryAfter(0)}
  />
)}
```

#### UI States

1. **Normal State** (no delay)
   - Login button enabled
   - No countdown shown

2. **Countdown State** (delay active)
   - Login button disabled with dimmed style
   - Countdown message: "⏳ Too many attempts. Please wait 15 seconds..."
   - Live countdown: "(12s remaining)"
   - Warning: "2 attempts remaining before lockout"

3. **Locked State** (account temporarily locked)
   - Login button disabled
   - Error message: "🔒 Account temporarily locked. Try again in 15 minutes."
   - Link to "Forgot Password?" for recovery

#### Acceptance Criteria
- [ ] Countdown decrements every second
- [ ] Button re-enables at 0
- [ ] Warning shows attempts remaining
- [ ] Locked state displays correctly

---

### Task 3: Integration & Edge Cases

#### Requirements
- [ ] Handle page refresh during countdown (persist to sessionStorage)
- [ ] Sync countdown with server time (handle clock drift)
- [ ] Handle multiple tabs (only one countdown)
- [ ] Clear countdown on successful login
- [ ] Handle network errors gracefully

#### Edge Cases to Test

| Scenario | Expected Behavior |
|----------|-------------------|
| User refreshes during countdown | Countdown resumes from sessionStorage |
| User opens new tab | Both tabs show same countdown |
| Network error during login | Show network error, don't increment attempts |
| Server clock drift | Use `retry_after` duration, not absolute time |
| Successful login after failures | Reset all counters, clear countdown |
| Password reset during lockout | Allow password reset, maintain lockout |

---

## Files to Modify

### Backend

| File | Changes |
|------|---------|
| `api/routers/auth.py` | Add delay calculation, update response |
| `api/services/auth_service.py` | Add `get_retry_delay()` method |
| `api/schemas/auth.py` | Add `retry_after`, `attempts_remaining` to response |

### Frontend

| File | Changes |
|------|---------|
| `src/app/login/page.tsx` | Handle retry_after, manage countdown state |
| `src/components/auth/login-countdown.tsx` | NEW - Countdown timer component |
| `src/hooks/useLoginCountdown.ts` | NEW - Hook for countdown logic |

---

## Testing Plan

### Unit Tests

```python
# Backend tests
def test_delay_calculation():
    assert calculate_delay(1) == 0
    assert calculate_delay(3) == 5
    assert calculate_delay(5) == 30
    assert calculate_delay(10) == 60

def test_early_retry_rejected():
    # Fail login 3 times
    # Immediately retry (within 5 seconds)
    # Assert 429 response with retry_after
```

### Integration Tests

```typescript
// Frontend tests
it('shows countdown after 3 failed attempts', async () => {
  // Fail login 3 times
  // Assert countdown is visible
  // Assert button is disabled
});

it('re-enables form after countdown', async () => {
  // Set countdown to 1 second
  // Wait 1 second
  // Assert button is enabled
});
```

### Manual Testing Checklist

- [ ] Fail login 3 times, verify 5-second delay
- [ ] Fail login 5 times, verify 30-second delay
- [ ] Refresh page during countdown, verify it resumes
- [ ] Successfully login after failures, verify counter resets
- [ ] Reach lockout, verify locked message

---

## Definition of Done

- [ ] Backend calculates and returns delay correctly
- [ ] Frontend displays countdown UI
- [ ] Button is disabled during countdown
- [ ] Countdown persists through refresh
- [ ] All tests pass
- [ ] Security review completed
- [ ] Documented in API docs

---

## Labels

`security` `authentication` `brute-force-protection` `skill: intermediate` `area: fullstack`

---

## Related Issues

- Rate Limiting Implementation (already done)
- Account Lockout (already done)
- Security Hardening (parent epic)
