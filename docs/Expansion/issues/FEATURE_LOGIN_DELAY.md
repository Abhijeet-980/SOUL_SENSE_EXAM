# Feature: Login Delay & Countdown on Failure

This document outlines the implementation plan for adding a progressive delay between failed login attempts to prevent brute-force attacks and improve security.

## 📋 Overview

**Goal:** Prevent rapid login attempts by enforcing a mandatory waiting period after repeated failures.
**Benefit:** Mitigates brute-force attacks and reduces server load from automated bots.

---

## 🛠️ Implementation Steps

### 📦 Backend: Enforce Delay Logic

**Description:**
Update the authentication service to track failed attempts and enforce a delay before allowing the next attempt.

**Tasks:**
1.  **Track Failures:**
    *   Store failed login attempts for each email/IP address (in-memory or Redis/DB).
    *   Increment a counter for each consecutive failure.
    *   Reset counter after a successful login or a set timeout (e.g., 15 minutes).

2.  **Calculate Delay:**
    *   Implement an exponential backoff or fixed step strategy.
    *   *Example:*
        *   Attempts 1-3: No delay
        *   Attempt 4: 30 seconds
        *   Attempt 5: 60 seconds
        *   Attempt 6+: 5 minutes

3.  **Enforce Wait Time:**
    *   Check if the user is currently in a "cooldown" period.
    *   If blocked, return a `429 Too Many Requests` status code.
    *   Include a `Retry-After` header or a JSON field `retry_after_seconds` indicating the remaining wait time.

**Technical Details:**
*   Modify `backend/fastapi/api/services/auth_service.py` (or equivalent).
*   Use the existing `RateLimiter` if applicable, or extend `LoginAttempt` logic.

---

### 📦 Frontend: Countdown UI

**Description:**
Update the login form to handle the `429` error and display a visual countdown timer to the user.

**Tasks:**
1.  **Handle 429 Response:**
    *   Update `frontend-web/src/app/(auth)/login/page.tsx`.
    *   Catch the `429` error from the login API call.
    *   Extract the `retry_after_seconds` value from the response.

2.  **Implement Countdown State:**
    *   Add a state variable `lockoutUntil` (timestamp) or `secondsRemaining`.
    *   Disable the "Sign In" button while the lockout is active.

3.  **Show Timer UI:**
    *   Display a warning message: "Too many failed attempts. Please try again in X seconds."
    *   Update "X" every second until it reaches 0.
    *   Re-enable the "Sign In" button when the timer ends.

**Mockup:**
> ⚠️ **Too many failed attempts**
> Please wait **00:28** before trying again.
> [ Sign In (Disabled) ]

---

## ✅ Acceptance Criteria

| Criteria | Verification |
| :--- | :--- |
| **Rapid Retries Blocked** | Attempting to login immediately after 3+ failures results in a 429 error. |
| **Delay Increases** | The wait time increases with subsequent failures (e.g., 30s -> 60s). |
| **UI Feedback** | User sees a clear error message explaining why they are blocked. |
| **Countdown Visible** | A real-time countdown timer is displayed. |
| **Button Disabled** | The submit button is disabled during the lockout period. |
| **Auto-Unlock** | The form becomes usable again automatically when the timer expires. |

---

## 🏷️ Issue Labels

`type: feature` `area: security` `area: frontend` `area: backend` `priority: high`
