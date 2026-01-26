# SoulSense API v1 - Complete Usage Guide

**Version:** 1.0.0  
**Last Updated:** January 26, 2026  
**Base URL:** `http://localhost:8000/api/v1`

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Endpoints Reference](#endpoints-reference)
   - [Health Check](#health-check)
   - [Authentication](#authentication-endpoints)
   - [Users](#users)
   - [Profiles](#profiles)
   - [Assessments](#assessments)
   - [Questions](#questions)
   - [Journal](#journal)
   - [Analytics](#analytics)
   - [Settings Sync](#settings-sync)
4. [Error Handling](#error-handling)
5. [Rate Limiting](#rate-limiting)
6. [Examples](#examples)

---

## Overview

The SoulSense API provides a comprehensive RESTful interface for the EQ (Emotional Quotient) Testing Platform. All versioned endpoints are accessible under the `/api/v1` prefix.

### Key Features

- **JWT Authentication** - Secure token-based authentication
- **Full CRUD Operations** - Complete resource management
- **Pagination Support** - Efficient data retrieval for large datasets
- **Sentiment Analysis** - AI-powered emotional analysis for journals
- **Rate Limiting** - Protection against abuse on analytics endpoints
- **Optimistic Locking** - Conflict detection for settings synchronization

### Headers

All API responses include:
```http
X-API-Version: 1.0
```

Authenticated requests require:
```http
Authorization: Bearer <access_token>
Content-Type: application/json
```

---

## Authentication

### Register a New User

```http
POST /api/v1/auth/register
```

**Request Body:**
```json
{
  "username": "johndoe",
  "password": "SecurePassword123!"
}
```

**Response (201 Created):**
```json
{
  "id": 1,
  "username": "johndoe",
  "created_at": "2026-01-26T10:30:00Z"
}
```

**Validation Rules:**
- Username: 3-50 characters, unique
- Password: Minimum 8 characters

---

### Login

```http
POST /api/v1/auth/login
```

**Request Body (Form Data):**
```
username=johndoe
password=SecurePassword123!
```

**Response (200 OK):**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

---

### Get Current User

```http
GET /api/v1/auth/me
Authorization: Bearer <token>
```

**Response:**
```json
{
  "id": 1,
  "username": "johndoe",
  "created_at": "2026-01-26T10:30:00Z"
}
```

---

## Endpoints Reference

### Health Check

Health endpoints are available at both root level and under `/api/v1`.

#### Liveness Probe
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2026-01-26T10:30:00Z",
  "version": "1.0.0"
}
```

#### Readiness Probe
```http
GET /ready?full=false
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `full` | boolean | false | Include detailed diagnostics |

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2026-01-26T10:30:00Z",
  "version": "1.0.0",
  "services": {
    "database": {
      "status": "healthy",
      "latency_ms": 1.25
    }
  }
}
```

#### Startup Probe
```http
GET /startup
```

Used for Kubernetes startupProbe to verify initialization.

---

### Users

All user endpoints require authentication.

#### Get Current User Info
```http
GET /api/v1/users/me
Authorization: Bearer <token>
```

**Response:**
```json
{
  "id": 1,
  "username": "johndoe",
  "created_at": "2026-01-26T10:30:00Z",
  "last_login": "2026-01-26T12:00:00Z"
}
```

#### Get Current User Details
```http
GET /api/v1/users/me/detail
Authorization: Bearer <token>
```

**Response:**
```json
{
  "id": 1,
  "username": "johndoe",
  "created_at": "2026-01-26T10:30:00Z",
  "last_login": "2026-01-26T12:00:00Z",
  "profile_completion": 75,
  "assessment_count": 12
}
```

#### Get Complete User Profile
```http
GET /api/v1/users/me/complete
Authorization: Bearer <token>
```

Returns all sub-profiles: settings, medical, personal, strengths, emotional patterns.

#### Update Current User
```http
PUT /api/v1/users/me
Authorization: Bearer <token>
```

**Request Body:**
```json
{
  "username": "newusername",
  "password": "NewSecurePassword123!"
}
```

#### Delete Current User
```http
DELETE /api/v1/users/me
Authorization: Bearer <token>
```

**Response:** `204 No Content`

⚠️ **Warning:** This action is irreversible and deletes all associated data.

#### List All Users
```http
GET /api/v1/users/
Authorization: Bearer <token>
```

---

### Profiles

Profile endpoints manage user settings and personal information. All require authentication.

#### User Settings

**Get Settings:**
```http
GET /api/v1/profiles/settings
```

**Create Settings:**
```http
POST /api/v1/profiles/settings
```

**Request Body:**
```json
{
  "theme": "dark",
  "question_count": 15,
  "sound_enabled": true,
  "notifications_enabled": true,
  "language": "en"
}
```

**Update Settings:**
```http
PUT /api/v1/profiles/settings
```

**Delete Settings:**
```http
DELETE /api/v1/profiles/settings
```

---

#### Medical Profile

**Get Medical Profile:**
```http
GET /api/v1/profiles/medical
```

**Create Medical Profile:**
```http
POST /api/v1/profiles/medical
```

**Request Body:**
```json
{
  "blood_type": "O+",
  "allergies": "Peanuts, Shellfish",
  "medications": "None",
  "medical_conditions": "None",
  "surgeries": "Appendectomy 2020",
  "therapy_history": "CBT 2023-2024",
  "ongoing_health_issues": "None",
  "emergency_contact_name": "Jane Doe",
  "emergency_contact_phone": "+1-555-123-4567"
}
```

**Update Medical Profile:**
```http
PUT /api/v1/profiles/medical
```

**Delete Medical Profile:**
```http
DELETE /api/v1/profiles/medical
```

---

#### Personal Profile

**Endpoints:**
- `GET /api/v1/profiles/personal`
- `POST /api/v1/profiles/personal`
- `PUT /api/v1/profiles/personal`
- `DELETE /api/v1/profiles/personal`

---

#### User Strengths

**Endpoints:**
- `GET /api/v1/profiles/strengths`
- `POST /api/v1/profiles/strengths`
- `PUT /api/v1/profiles/strengths`
- `DELETE /api/v1/profiles/strengths`

---

#### Emotional Patterns

**Endpoints:**
- `GET /api/v1/profiles/emotional`
- `POST /api/v1/profiles/emotional`
- `PUT /api/v1/profiles/emotional`
- `DELETE /api/v1/profiles/emotional`

---

### Assessments

#### List Assessments
```http
GET /api/v1/assessments
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `username` | string | - | Filter by username |
| `age_group` | string | - | Filter by age group (e.g., "18-25") |
| `page` | int | 1 | Page number (starts at 1) |
| `page_size` | int | 10 | Items per page (max 100) |

**Response:**
```json
{
  "total": 45,
  "assessments": [
    {
      "id": 1,
      "username": "johndoe",
      "total_score": 35,
      "sentiment_score": 0.75,
      "is_rushed": false,
      "is_inconsistent": false,
      "age": 25,
      "detailed_age_group": "18-25",
      "timestamp": "2026-01-22T10:30:00Z"
    }
  ],
  "page": 1,
  "page_size": 10
}
```

#### Get Assessment Details
```http
GET /api/v1/assessments/{assessment_id}
```

**Response:**
```json
{
  "id": 1,
  "username": "johndoe",
  "total_score": 35,
  "sentiment_score": 0.75,
  "reflection_text": "I felt calm and focused during the assessment...",
  "is_rushed": false,
  "is_inconsistent": false,
  "age": 25,
  "detailed_age_group": "18-25",
  "timestamp": "2026-01-22T10:30:00Z",
  "responses_count": 10
}
```

#### Get Assessment Statistics
```http
GET /api/v1/assessments/stats
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `username` | string | Filter stats for specific user |

**Response:**
```json
{
  "total_assessments": 45,
  "average_score": 34.5,
  "highest_score": 40,
  "lowest_score": 28,
  "average_sentiment": 0.68,
  "age_group_distribution": {
    "18-25": 20,
    "26-35": 15,
    "36-50": 10
  }
}
```

---

### Questions

#### List Questions
```http
GET /api/v1/questions
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `age` | int | - | Filter questions appropriate for this age (10-120) |
| `category_id` | int | - | Filter by category |
| `limit` | int | 100 | Maximum questions (max 200) |
| `skip` | int | 0 | Number to skip for pagination |
| `active_only` | bool | true | Only return active questions |

**Response:**
```json
{
  "total": 45,
  "questions": [
    {
      "id": 1,
      "question_text": "How well do you handle stress in difficult situations?",
      "category_id": 1,
      "difficulty": 2,
      "min_age": 18,
      "max_age": 120,
      "weight": 1.0,
      "tooltip": "Think about recent stressful situations",
      "is_active": true
    }
  ],
  "page": 1,
  "page_size": 100
}
```

#### Get Questions by Age
```http
GET /api/v1/questions/by-age/{age}
```

**Path Parameters:**
- `age`: User's age (10-120)

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `limit` | int | Optional limit on number of questions (max 200) |

**Response:** Array of question objects

#### Get Single Question
```http
GET /api/v1/questions/{question_id}
```

#### List Categories
```http
GET /api/v1/questions/categories
```

**Response:**
```json
[
  {"id": 1, "name": "Self-Awareness"},
  {"id": 2, "name": "Emotional Regulation"},
  {"id": 3, "name": "Empathy"},
  {"id": 4, "name": "Social Skills"},
  {"id": 5, "name": "Motivation"}
]
```

#### Get Category by ID
```http
GET /api/v1/questions/categories/{category_id}
```

---

### Journal

All journal endpoints require authentication.

#### Create Journal Entry
```http
POST /api/v1/journal
Authorization: Bearer <token>
```

**Request Body:**
```json
{
  "content": "Today I felt really productive and managed my emotions well...",
  "tags": ["productivity", "emotions", "work"],
  "privacy_level": "private",
  "sleep_hours": 7.5,
  "sleep_quality": 4,
  "energy_level": 4,
  "work_hours": 8,
  "screen_time_mins": 180,
  "stress_level": 2,
  "stress_triggers": "deadline",
  "daily_schedule": "normal"
}
```

**Response (201 Created):**
```json
{
  "id": 1,
  "content": "Today I felt really productive...",
  "tags": ["productivity", "emotions", "work"],
  "privacy_level": "private",
  "sentiment_score": 0.82,
  "word_count": 45,
  "created_at": "2026-01-26T14:30:00Z",
  "updated_at": "2026-01-26T14:30:00Z"
}
```

#### List Journal Entries
```http
GET /api/v1/journal
Authorization: Bearer <token>
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `skip` | int | 0 | Number of entries to skip |
| `limit` | int | 20 | Entries per page (max 100) |
| `start_date` | string | - | Filter by start date (YYYY-MM-DD) |
| `end_date` | string | - | Filter by end date (YYYY-MM-DD) |

#### Get Single Journal Entry
```http
GET /api/v1/journal/{journal_id}
Authorization: Bearer <token>
```

#### Update Journal Entry
```http
PUT /api/v1/journal/{journal_id}
Authorization: Bearer <token>
```

Sentiment is re-analyzed if content changes.

#### Delete Journal Entry
```http
DELETE /api/v1/journal/{journal_id}
Authorization: Bearer <token>
```

**Response:** `204 No Content`

#### Search Journal Entries
```http
GET /api/v1/journal/search
Authorization: Bearer <token>
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | string | Search query (min 2 chars) |
| `tags` | array | Filter by tags |
| `min_sentiment` | float | Minimum sentiment score (0-100) |
| `max_sentiment` | float | Maximum sentiment score (0-100) |
| `skip` | int | Pagination offset |
| `limit` | int | Results per page (max 100) |

#### Get Journal Analytics
```http
GET /api/v1/journal/analytics
Authorization: Bearer <token>
```

**Response:**
```json
{
  "total_entries": 45,
  "average_sentiment": 0.72,
  "sentiment_trend": "improving",
  "most_common_tags": ["work", "family", "health"],
  "writing_frequency": {
    "daily_average": 1.2,
    "weekly_average": 8.4
  },
  "mood_distribution": {
    "positive": 60,
    "neutral": 30,
    "negative": 10
  }
}
```

#### Get AI Prompts
```http
GET /api/v1/journal/prompts
```

**Query Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| `category` | string | Filter by: gratitude, reflection, goals, emotions, creativity |

**Response:**
```json
{
  "prompts": [
    {
      "text": "What are three things you're grateful for today?",
      "category": "gratitude"
    }
  ],
  "category": "gratitude"
}
```

#### Export Journal Entries
```http
GET /api/v1/journal/export
Authorization: Bearer <token>
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `format` | string | json | Export format: json, txt |
| `start_date` | string | - | Filter by start date |
| `end_date` | string | - | Filter by end date |

---

### Analytics

Analytics endpoints return only aggregated, non-sensitive data. All are rate-limited to 30 requests per minute per IP.

#### Get Summary
```http
GET /api/v1/analytics/summary
```

**Response:**
```json
{
  "total_assessments": 1250,
  "unique_users": 450,
  "global_average_score": 34.2,
  "age_group_statistics": {...},
  "score_distribution": {...},
  "quality_metrics": {...}
}
```

#### Get Trends
```http
GET /api/v1/analytics/trends
```

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `period` | string | monthly | Time period: daily, weekly, monthly |
| `limit` | int | 12 | Number of periods (max 24) |

**Response:**
```json
{
  "period_type": "monthly",
  "data": [
    {"period": "2026-01", "average_score": 34.5, "count": 120},
    {"period": "2025-12", "average_score": 33.8, "count": 115}
  ],
  "trend_direction": "improving"
}
```

#### Get Benchmarks
```http
GET /api/v1/analytics/benchmarks
```

**Response:**
```json
[
  {
    "metric": "total_score",
    "global_average": 34.2,
    "percentile_25": 28,
    "percentile_50": 34,
    "percentile_75": 38,
    "percentile_90": 42
  }
]
```

#### Get Population Insights
```http
GET /api/v1/analytics/insights
```

**Response:**
```json
{
  "most_common_age_group": "26-35",
  "highest_performing_age_group": "36-50",
  "total_population": 450,
  "completion_rate": 0.85
}
```

#### Get Age Group Statistics
```http
GET /api/v1/analytics/age-groups
```

#### Get Score Distribution
```http
GET /api/v1/analytics/distribution
```

---

### Settings Sync

Settings synchronization endpoints with optimistic locking support. All require authentication.

#### Get All Settings
```http
GET /api/v1/sync/
Authorization: Bearer <token>
```

**Response:**
```json
[
  {
    "key": "theme",
    "value": "dark",
    "version": 3,
    "updated_at": "2026-01-26T10:30:00Z"
  },
  {
    "key": "language",
    "value": "en",
    "version": 1,
    "updated_at": "2026-01-25T08:00:00Z"
  }
]
```

#### Get Single Setting
```http
GET /api/v1/sync/{key}
Authorization: Bearer <token>
```

#### Upsert Setting
```http
PUT /api/v1/sync/{key}
Authorization: Bearer <token>
```

**Request Body:**
```json
{
  "value": "dark",
  "expected_version": 2
}
```

The `expected_version` field enables optimistic locking. If provided, the update only succeeds if the current version matches.

**Response (200 OK):**
```json
{
  "key": "theme",
  "value": "dark",
  "version": 3,
  "updated_at": "2026-01-26T10:30:00Z"
}
```

**Response (409 Conflict):**
```json
{
  "detail": {
    "message": "Version conflict: expected 2, found 3",
    "key": "theme",
    "current_version": 3,
    "current_value": "light"
  }
}
```

#### Delete Setting
```http
DELETE /api/v1/sync/{key}
Authorization: Bearer <token>
```

**Response:** `204 No Content`

#### Batch Upsert Settings
```http
POST /api/v1/sync/batch
Authorization: Bearer <token>
```

**Request Body:**
```json
{
  "settings": [
    {"key": "theme", "value": "dark"},
    {"key": "language", "value": "hi"},
    {"key": "notifications", "value": true}
  ]
}
```

**Response:**
```json
{
  "settings": [
    {"key": "theme", "value": "dark", "version": 4, "updated_at": "..."},
    {"key": "language", "value": "hi", "version": 2, "updated_at": "..."}
  ],
  "conflicts": ["notifications"]
}
```

---

## Error Handling

### Standard HTTP Status Codes

| Code | Meaning |
|------|---------|
| 200 | OK - Request successful |
| 201 | Created - Resource created |
| 204 | No Content - Successful deletion |
| 400 | Bad Request - Invalid parameters |
| 401 | Unauthorized - Missing or invalid token |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource not found |
| 409 | Conflict - Version mismatch (settings sync) |
| 422 | Unprocessable Entity - Validation error |
| 429 | Too Many Requests - Rate limit exceeded |
| 500 | Internal Server Error |
| 503 | Service Unavailable - Dependency failure |

### Error Response Format

```json
{
  "detail": "Error description message"
}
```

Validation errors:
```json
{
  "detail": [
    {
      "loc": ["body", "password"],
      "msg": "String should have at least 8 characters",
      "type": "string_too_short"
    }
  ]
}
```

---

## Rate Limiting

Analytics endpoints are rate-limited to **30 requests per minute per IP**.

When rate limited, you'll receive:
- Status: `429 Too Many Requests`
- Header: `Retry-After: 60`

---

## Examples

### Complete Authentication Flow (Python)

```python
import requests

BASE_URL = "http://localhost:8000/api/v1"

# 1. Register
response = requests.post(f"{BASE_URL}/auth/register", json={
    "username": "testuser",
    "password": "SecurePassword123!"
})
print(f"Registered: {response.json()}")

# 2. Login
response = requests.post(f"{BASE_URL}/auth/login", data={
    "username": "testuser",
    "password": "SecurePassword123!"
})
token = response.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# 3. Get current user
response = requests.get(f"{BASE_URL}/users/me", headers=headers)
print(f"Current user: {response.json()}")

# 4. Create a journal entry
response = requests.post(f"{BASE_URL}/journal", headers=headers, json={
    "content": "Today was a productive day...",
    "tags": ["productivity", "work"]
})
print(f"Journal created: {response.json()}")
```

### cURL Examples

```bash
# Health check
curl http://localhost:8000/health

# Register
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"testuser","password":"SecurePass123!"}'

# Login
curl -X POST http://localhost:8000/api/v1/auth/login \
  -d "username=testuser&password=SecurePass123!"

# Get questions for age 25
curl "http://localhost:8000/api/v1/questions?age=25&limit=10"

# Get assessments with pagination
curl "http://localhost:8000/api/v1/assessments?page=1&page_size=5"

# Create journal (authenticated)
curl -X POST http://localhost:8000/api/v1/journal \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content":"My journal entry...","tags":["daily"]}'

# Sync setting with conflict detection
curl -X PUT http://localhost:8000/api/v1/sync/theme \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"value":"dark","expected_version":2}'
```

---

## Interactive Documentation

When the server is running:

- **Swagger UI:** http://localhost:8000/docs
- **ReDoc:** http://localhost:8000/redoc
- **API Version Discovery:** http://localhost:8000/

---

## Related Documentation

- [API Versioning Strategy](API_VERSIONING.md)
- [Postman Test Guide](POSTMAN_GUIDE.md)
- [Troubleshooting Guide](../guides/TROUBLESHOOTING.md)
- [Quick Start Guide](../guides/QUICKSTART.md)

---

**Built with FastAPI** | **Authentication: JWT/OAuth2** | **Database: SQLite/PostgreSQL**
