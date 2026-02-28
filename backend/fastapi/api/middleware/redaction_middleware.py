import json
import logging
from typing import List, Optional
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from ..utils.redaction import redact_data
from ..config import get_settings_instance

logger = logging.getLogger(__name__)

async def redaction_middleware(request: Request, call_next):
    """
    Post-processing middleware to redact PII from JSON responses (#1088).
    Checks user roles from request.state.user.roles or similar.
    """
    response: Response = await call_next(request)
    
    # 1. Bypass if not JSON or if it's an internal system call
    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        return response

    # 2. Get user roles
    user = getattr(request.state, "user", None)
    roles = []
    if user:
        if getattr(user, "is_admin", False):
            roles.append("admin")
        # Extend with other roles from DB/Session if available
    
    # 3. Intercept and redact
    # Using streaming response or direct body? Direct body for medium/small JSON is easier.
    # Note: For very large responses, this can be memory-intensive.
    body = [chunk async for chunk in response.body_iterator]
    response.body_iterator = iterate_in_threadpool(iter(body))
    
    try:
        data = json.loads(b"".join(body))
        redacted_data = redact_data(data, roles)
        
        # Replace the response body
        new_content = json.dumps(redacted_data).encode("utf-8")
        from fastapi.responses import Response as FAResponse
        return FAResponse(
            content=new_content,
            status_code=response.status_code,
            headers=dict(response.headers)
        )
    except Exception as e:
        logger.debug(f"Redaction middleware skipped non-json or malformed content: {e}")
        return response

from starlette.concurrency import iterate_in_threadpool
