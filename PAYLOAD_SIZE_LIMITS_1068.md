# Payload Size Limits and DoS Protection (Issue #1068)

## Overview

This implementation adds comprehensive payload size limits and DoS (Denial of Service) protection to the SoulSense API. It prevents backend crashes due to oversized or malformed payloads by enforcing configurable limits on request body size, JSON nesting depth, array/object sizes, and detecting compression bombs.

## Features

### 1. Request Body Size Limits
- **Max Request Size**: Configurable maximum request body size (default: 10MB)
- **Content-Length Validation**: Early rejection based on Content-Length header
- **Streaming Validation**: Real-time size checking during body read

### 2. JSON Payload Validation
- **Nesting Depth**: Maximum JSON nesting depth (default: 20 levels)
- **Array Size**: Maximum elements in JSON arrays (default: 10,000)
- **Object Keys**: Maximum keys in JSON objects (default: 1,000)
- **Structure Validation**: Comprehensive structural integrity checks

### 3. Compression Bomb Detection
- **Gzip Detection**: Identifies gzip compression bombs
- **Zip Detection**: Identifies zip archive bombs
- **Ratio Threshold**: Configurable compression ratio threshold (default: 10:1)
- **Size Limits**: Maximum uncompressed size checks

### 4. Multipart Form Validation
- **Part Limits**: Maximum number of parts in multipart requests (default: 100)
- **File Size**: Maximum file upload size (default: 50MB)
- **Boundary Validation**: Proper multipart boundary checking

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_REQUEST_SIZE_BYTES` | 10485760 (10MB) | Maximum request body size |
| `MAX_JSON_DEPTH` | 20 | Maximum JSON nesting depth |
| `MAX_MULTIPART_PARTS` | 100 | Maximum multipart parts |
| `MAX_MULTIPART_FILE_SIZE_BYTES` | 52428800 (50MB) | Maximum file upload size |
| `MAX_ARRAY_SIZE` | 10000 | Maximum JSON array elements |
| `MAX_OBJECT_KEYS` | 1000 | Maximum JSON object keys |
| `ENABLE_COMPRESSION_BOMB_CHECK` | true | Enable compression bomb detection |
| `COMPRESSION_BOMB_RATIO` | 10.0 | Compression ratio threshold |

### Programmatic Configuration

Configuration is handled through the Pydantic settings in `backend/fastapi/api/config.py`:

```python
from api.config import get_settings_instance

settings = get_settings_instance()
print(f"Max request size: {settings.max_request_size_bytes} bytes")
print(f"Max JSON depth: {settings.max_json_depth}")
```

## Error Codes

| Code | Description | HTTP Status |
|------|-------------|-------------|
| `DOS001` | Payload too large | 413 |
| `DOS002` | JSON depth exceeded | 413 |
| `DOS003` | Malformed payload | 400 |
| `DOS004` | Compression bomb detected | 413 |
| `DOS005` | Too many multipart parts | 413 |

## Error Response Format

```json
{
  "code": "DOS001",
  "message": "Request body too large: 15728640 bytes (max: 10485760 bytes)",
  "details": {
    "size_bytes": 15728640,
    "max_size_bytes": 10485760,
    "size_mb": 15.0,
    "max_size_mb": 10.0
  }
}
```

## Implementation Details

### Middleware Architecture

The `PayloadLimitMiddleware` is added as the outermost middleware in the FastAPI application stack to block oversized requests as early as possible:

```python
# In backend/fastapi/api/main.py
from .middleware.payload_limit_middleware import PayloadLimitMiddleware
app.add_middleware(PayloadLimitMiddleware)
```

### Validation Flow

1. **Path Exclusion Check**: Skip validation for health checks and static files
2. **Content-Length Check**: Validate header before reading body
3. **Body Read with Limit**: Stream body with real-time size checking
4. **Content-Type Validation**: Apply specific validation based on content type
5. **Structure Validation**: Validate JSON/array/object structure
6. **Bomb Detection**: Check for compression bombs

### Excluded Paths

The following paths are excluded from payload validation:
- `/health`, `/healthz`, `/ready`, `/alive`, `/metrics`
- `/favicon.ico`
- `/docs`, `/redoc`, `/openapi.json`
- `/static/` (static files)

## Usage Examples

### Valid Request

```bash
curl -X POST "http://localhost:8000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'
```

Response: `200 OK`

### Oversized Payload

```bash
curl -X POST "http://localhost:8000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'print({\"data\": \"x\"*20000000})')"
```

Response: `413 Payload Too Large`
```json
{
  "code": "DOS001",
  "message": "Request body too large: 20000015 bytes (max: 10485760 bytes)",
  "details": {
    "size_bytes": 20000015,
    "max_size_bytes": 10485760
  }
}
```

### Deeply Nested JSON

```bash
curl -X POST "http://localhost:8000/api/v1/users" \
  -H "Content-Type: application/json" \
  -d '{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":{"l7":{"l8":{"l9":{"l10":{"l11":{"l12":{"l13":{"l14":{"l15":{"l16":{"l17":{"l18":{"l19":{"l20":{"l21":"deep"}}}}}}}}}}}}}}}}}}}}}'
```

Response: `413 Payload Too Large`
```json
{
  "code": "DOS002",
  "message": "JSON nesting depth exceeded: 21 (max: 20)",
  "details": {
    "depth": 21,
    "max_depth": 20
  }
}
```

## Testing

### Unit Tests

```bash
cd backend/fastapi
pytest tests/unit/test_payload_limits_1068.py -v
```

### Integration Tests

```bash
cd backend/fastapi
pytest tests/unit/test_payload_limit_middleware_1068.py -v
```

### Test Coverage

- ✅ Payload size validation
- ✅ JSON depth validation
- ✅ Array/object size validation
- ✅ Compression bomb detection (gzip)
- ✅ Compression bomb detection (zip)
- ✅ Multipart part limits
- ✅ Error response format
- ✅ Excluded paths
- ✅ Configuration loading

## Security Considerations

### Edge Cases Handled

1. **50MB JSON Body**: Rejected by size limit before parsing
2. **Deep Nested Arrays**: Rejected by depth validation
3. **Compression Bombs**: Detected via ratio analysis
4. **Multipart Abuse**: Limited by part count
5. **Malformed Payloads**: Caught by validation error handlers

### Performance Impact

- Minimal overhead for valid requests
- Early rejection prevents resource exhaustion
- Streaming validation avoids memory spikes
- Excluded paths have zero overhead

## Monitoring and Logging

All payload violations are logged with:
- Request ID for tracing
- Violation type and details
- Client IP (if available)
- Request path

Example log entry:
```
WARNING:api.payload_limit:Request body exceeded size limit: 15728640 bytes (max: 10485760 bytes) [request_id=abc-123 path=/api/v1/upload]
```

## Future Enhancements

Potential improvements for future iterations:

1. **Rate Limiting Integration**: Combine with rate limiting for comprehensive DoS protection
2. **IP-based Limits**: Different limits for authenticated vs unauthenticated users
3. **Graduated Responses**: Warnings before hard rejections
4. **Metrics**: Prometheus metrics for payload violations
5. **Machine Learning**: ML-based anomaly detection for unusual payload patterns

## References

- Issue: #1068
- OWASP DoS Protection: https://owasp.org/www-community/attacks/Denial_of_Service
- FastAPI Middleware: https://fastapi.tiangolo.com/tutorial/middleware/
- RFC 7231 (HTTP/1.1): https://tools.ietf.org/html/rfc7231

## Changelog

### Version 1.0.0
- Initial implementation
- Payload size limits
- JSON depth validation
- Compression bomb detection
- Multipart validation
- Comprehensive test coverage
