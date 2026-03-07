# 🚀 Pull Request: Row-Level TTL Archival Partitioning (#1413)

## 📝 Description

This PR implements a comprehensive row-level TTL (Time-To-Live) archival partitioning system for database data lifecycle management. The system provides configurable retention policies, automated archival operations, and safe rollout controls to manage database growth and ensure compliance with data retention requirements.

- **Objective**: Implement measurable improvement in database quality with configurable TTL policies, automated archival, and comprehensive observability.
- **Context**: Addresses the need for systematic data retention management to reduce storage costs, improve query performance, and meet compliance requirements.

**Closes #1413**

---

## 🔧 Type of Change

Mark the relevant options:

- [ ] 🐛 **Bug Fix**: A non-breaking change which fixes an issue.
- [x] ✨ **New Feature**: A non-breaking change which adds functionality.
- [ ] 💥 **Breaking Change**: A fix or feature that would cause existing functionality to not work as expected.
- [ ] ♻️ **Refactor**: Code improvement (no functional changes).
- [x] 📝 **Documentation Update**: Changes to README, comments, or external docs.
- [x] 🚀 **Performance / Security**: Improvements to app speed or security posture.

---

## 🧪 How Has This Been Tested?

Describe the tests you ran to verify your changes. Include steps to reproduce if necessary.

- [x] **Unit Tests**: Ran comprehensive unit tests covering policy management, archival strategies, and statistics tracking.
- [x] **Integration Tests**: Verified database operations and end-to-end TTL workflows.
- [x] **Manual Verification**: Tested API endpoints and background task execution.

### Test Coverage

**Unit Tests** (`tests/unit/test_ttl_partition_manager.py`):
- TTLPolicy dataclass validation (creation, serialization)
- ArchivalStats calculation and error handling
- PartitionInfo data structure
- TTLPartitionManager initialization
- Policy registration and retrieval
- All archival strategies (DELETE, SOFT_DELETE, ARCHIVE_THEN_DELETE, ARCHIVE_ONLY)
- Statistics aggregation and history tracking
- Callback registration system

**Integration Tests** (`tests/integration/test_ttl_archival_integration.py`):
- Manager initialization with real database
- Policy CRUD operations
- Archival execution with dry-run mode
- Historical data tracking
- Statistics aggregation
- Different retention periods and batch sizes
- Global manager instance handling

### Test Execution

```bash
# Run unit tests
cd backend/fastapi
python -m pytest tests/unit/test_ttl_partition_manager.py -v

# Run integration tests
python -m pytest tests/integration/test_ttl_archival_integration.py -v

# Run all TTL-related tests
python -m pytest tests/ -k "ttl" -v
```

---

## 📸 Screenshots / Recordings (if applicable)

### API Endpoints

```bash
# Get TTL system status
GET /api/v1/admin/ttl/status

# Response:
{
  "status": "healthy",
  "policies": [
    {
      "table_name": "notification_logs",
      "retention_days": 90,
      "archive_strategy": "archive_then_delete",
      "status": "active"
    }
  ],
  "statistics": {
    "policy_count": 5,
    "active_policies": 4,
    "total_rows_archived": 150000,
    "total_rows_deleted": 75000,
    "runs_last_24h": 3
  }
}
```

### Archival Run Results

```bash
# Run archival for a table
POST /api/v1/admin/ttl/policies/notification_logs/run
{"dry_run": false}

# Response:
{
  "table_name": "notification_logs",
  "start_time": "2026-03-07T10:00:00",
  "end_time": "2026-03-07T10:05:30",
  "rows_scanned": 50000,
  "rows_archived": 25000,
  "rows_deleted": 25000,
  "duration_ms": 330000,
  "success": true,
  "errors": []
}
```

---

## ✅ Checklist

Confirm you have completed the following steps:

- [x] My code follows the project's style guidelines.
- [x] I have performed a self-review of my code.
- [x] I have added/updated necessary comments or documentation.
- [x] My changes generate no new warnings or linting errors.
- [x] Existing tests pass with my changes.
- [x] I have verified this PR on the latest `main` branch.

---

## 🔒 Security Checklist (required for security-related PRs)

> **Reference:** [docs/SECURITY_HARDENING_CHECKLIST.md](docs/SECURITY_HARDENING_CHECKLIST.md)

- [x] `python scripts/check_security_hardening.py` passes — all required checks ✅
- [x] Relevant rows in the [Security Hardening Checklist](docs/SECURITY_HARDENING_CHECKLIST.md) are updated
- [x] No new secrets committed to the repository
- [x] New endpoints have rate limiting and input validation
- [x] Security-focused review requested from at least one maintainer

### Security Considerations

1. **Admin-only Access**: All TTL management endpoints require admin privileges via `require_admin` dependency.
2. **Dry-run Mode**: All archival operations support dry-run mode for safe testing.
3. **Batch Processing**: Large deletions are processed in batches to prevent long-running transactions.
4. **Audit Trail**: All archival operations are logged with statistics and errors.
5. **Input Validation**: Pydantic models enforce type safety and constraints.
6. **SQL Injection Prevention**: All SQL queries use parameterized statements.

---

## 📝 Additional Notes

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    TTL Management System                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Policies   │  │  Archival    │  │  Partitions  │      │
│  │              │  │   Engine     │  │              │      │
│  │ • retention  │  │              │  │ • monthly    │      │
│  │ • strategy   │  │ • dry-run    │  │ • quarterly  │      │
│  │ • filters    │  │ • batching   │  │ • yearly     │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│         └──────────────────┼──────────────────┘              │
│                            │                                 │
│                   ┌────────▼────────┐                        │
│                   │  TTL Partition  │                        │
│                   │     Manager     │                        │
│                   └────────┬────────┘                        │
│                            │                                 │
│    ┌───────────────────────┼───────────────────────┐        │
│    │                       │                       │        │
│ ┌──▼────┐            ┌────▼────┐           ┌──────▼────┐   │
│ │  API  │            │ Celery  │           │   Stats   │   │
│ │Router │            │  Tasks  │           │   & History│   │
│ └───────┘            └─────────┘           └───────────┘   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Key Features

| Feature | Description |
|---------|-------------|
| **Configurable Policies** | Per-table TTL with retention days, granularity, and strategy |
| **Archive Strategies** | DELETE, SOFT_DELETE, ARCHIVE_THEN_DELETE, ARCHIVE_ONLY |
| **Dry-run Mode** | Safe testing without actual data modification |
| **Batch Processing** | Configurable batch sizes for large datasets |
| **Partition Management** | Time-based partition tracking and visualization |
| **Observability** | Comprehensive metrics, history, and audit trails |
| **Background Execution** | Celery tasks for scheduled archival operations |

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/admin/ttl/status` | System status and overview |
| GET | `/api/v1/admin/ttl/policies` | List all policies |
| POST | `/api/v1/admin/ttl/policies` | Create/update policy |
| GET | `/api/v1/admin/ttl/policies/{table}` | Get specific policy |
| PATCH | `/api/v1/admin/ttl/policies/{table}/status` | Update policy status |
| DELETE | `/api/v1/admin/ttl/policies/{table}` | Delete policy |
| POST | `/api/v1/admin/ttl/policies/{table}/run` | Run archival for table |
| POST | `/api/v1/admin/ttl/run-all` | Run all active policies |
| GET | `/api/v1/admin/ttl/partitions/{table}` | Get partition info |
| GET | `/api/v1/admin/ttl/history` | Archival history |
| GET | `/api/v1/admin/ttl/statistics` | Overall statistics |

### Configuration Example

```python
# Create TTL policy
policy = TTLPolicy(
    table_name="notification_logs",
    retention_days=90,
    partition_granularity=PartitionGranularity.MONTHLY,
    archive_strategy=ArchiveStrategy.ARCHIVE_THEN_DELETE,
    archive_table="notification_logs_archive",
    batch_size=1000,
    filters={"status": "completed"},  # Only archive completed logs
)

# Register policy
await manager.register_policy(policy)

# Run archival
stats = await manager.archive_expired_data(policy)
print(f"Archived {stats.rows_archived} rows in {stats.duration_ms}ms")
```

### Celery Beat Schedule

```python
CELERY_BEAT_SCHEDULE = {
    'ttl-archival-daily': {
        'task': 'api.celery_tasks_ttl.run_all_ttl_policies_task',
        'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM
    },
    'ttl-cleanup-weekly': {
        'task': 'api.celery_tasks_ttl.cleanup_archived_data_task',
        'schedule': crontab(hour=3, minute=0, day_of_week='sunday'),
    },
}
```

### Edge Cases Handled

1. **Empty Tables**: Gracefully handles tables with no data to archive
2. **Large Datasets**: Batch processing prevents memory issues
3. **Concurrent Access**: Transaction-safe operations
4. **Failed Archives**: Error tracking and retry mechanisms
5. **Schema Changes**: Dynamic archive table creation
6. **Policy Conflicts**: Last-write-wins for policy updates
7. **Database Errors**: Proper rollback and error reporting

### Rollback Plan

- All archival operations support dry-run mode
- SOFT_DELETE strategy allows data recovery
- Archive tables preserve data before deletion
- Policy status can be changed to PAUSED at any time

### Performance Impact

- **Minimal Overhead**: Archival runs during off-peak hours
- **Batch Processing**: Configurable batch sizes (default 1000)
- **Indexing**: Efficient queries using timestamp indexes
- **Async Operations**: Non-blocking archival execution

---

## Files Changed

```
backend/fastapi/api/utils/ttl_partition_manager.py      (NEW)
backend/fastapi/api/routers/ttl_management.py           (NEW)
backend/fastapi/api/celery_tasks_ttl.py                 (NEW)
backend/fastapi/tests/unit/test_ttl_partition_manager.py (NEW)
backend/fastapi/tests/integration/test_ttl_archival_integration.py (NEW)
backend/fastapi/api/main.py                              (MODIFIED)
```

**Total**: 6 files, ~3,500 lines added

---

## Deployment Notes

1. **Database Migrations**: Automatic table creation on first startup
2. **Configuration**: No required configuration changes
3. **Monitoring**: New metrics available at `/api/v1/admin/ttl/statistics`
4. **Scheduling**: Configure Celery beat for automated execution

---

*Branch: `fix/row-level-ttl-archival-partitioning-1413`*
*Tests: 50+ test cases, all passing*
*Documentation: Complete API docs and examples*
