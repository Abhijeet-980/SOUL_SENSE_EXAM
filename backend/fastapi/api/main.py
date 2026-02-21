from fastapi import FastAPI, Request
import asyncio
import logging
import traceback
import uuid
import time
from fastapi.responses import JSONResponse
# Triggering reload for new community routes
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from .config import get_settings_instance
from .api.v1.router import api_router as api_v1_router
from .routers.health import router as health_router

# Load and validate settings on import
settings = get_settings_instance()


class VersionHeaderMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["X-API-Version"] = "1.0"
        return response


class PerformanceMonitoringMiddleware(BaseHTTPMiddleware):
    """
    Middleware to track API response times and performance metrics.
    Logs slow requests and adds performance headers.
    """
    async def dispatch(self, request: Request, call_next):
        start_time = time.time()

        # Process request
        response = await call_next(request)

        # Calculate duration
        process_time = (time.time() - start_time) * 1000  # Convert to milliseconds

        # Add performance header
        response.headers["X-Process-Time"] = f"{process_time:.2f}"

        # Log slow requests (> 500ms)
        if process_time > 500:
            logger = logging.getLogger("api.performance")
            logger.warning(
                f"Slow request: {request.method} {request.url.path} took {process_time:.2f}ms"
            )

        # Log all requests in debug mode
        settings = get_settings_instance()
        if settings.debug:
            logger = logging.getLogger("api.requests")
            logger.info(
                f"{request.method} {request.url.path} - Status: {response.status_code} - Time: {process_time:.2f}ms"
            )

        return response


def create_app() -> FastAPI:
    app = FastAPI(
        title="SoulSense API",
        description="Comprehensive REST API for SoulSense EQ Test Platform",
        version="1.0.0",
        docs_url="/docs",
        redoc_url="/redoc"
    )

    # Performance Monitoring Middleware (inner-most for accurate timing)
    app.add_middleware(PerformanceMonitoringMiddleware)

    # GZip compression middleware for response optimization
    app.add_middleware(GZipMiddleware, minimum_size=1000, compresslevel=6)

    # Security Headers Middleware
    from .middleware.security import SecurityHeadersMiddleware
    app.add_middleware(SecurityHeadersMiddleware)

    # Register V1 API Router
    app.include_router(api_v1_router, prefix="/api/v1")

    # Register Health endpoints at root level for orchestration
    app.include_router(health_router, tags=["Health"])

    # Version header middleware
    app.add_middleware(VersionHeaderMiddleware)

    # CORS middleware (Outer-most)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:3005", "http://localhost:3005", "tauri://localhost", "http://localhost:1420", "http://localhost:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    from .exceptions import APIException
    from .constants.errors import ErrorCode

    @app.exception_handler(APIException)
    async def api_exception_handler(request: Request, exc: APIException):
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail
        )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger = logging.getLogger("api.main")
        logger.error(f"Global Exception: {exc}")
        traceback.print_exc()
        
        # Don't leak raw exception info in production
        error_msg = str(exc) if settings.debug else "An unexpected error occurred"
        
        return JSONResponse(
            status_code=500,
            content={
                "code": ErrorCode.INTERNAL_SERVER_ERROR.value,
                "message": "Internal Server Error",
                "details": {"error": error_msg} if settings.debug else None
            }
        )

    # Root endpoint - version discovery
    @app.get("/", tags=["Root"])
    async def root():
        return {
            "name": "SoulSense API",
            "versions": [
                {"version": "v1", "status": "current", "path": "/api/v1"}
            ],
            "documentation": "/docs"
        }

    @app.on_event("startup")
    async def startup_event():
        app.state.settings = settings
        
        # Generate a unique instance ID for this server session
        # All JWTs will include this ID; tokens from previous instances are rejected
        app.state.server_instance_id = str(uuid.uuid4())
        print(f"[OK] Server instance ID: {app.state.server_instance_id}")
        
        # Initialize database tables
        try:
            from .services.db_service import Base, engine, SessionLocal
            Base.metadata.create_all(bind=engine)
            print("[OK] Database tables initialized/verified")
            
            # Start background task for soft-delete cleanup
            async def purge_task_loop():
                while True:
                    try:
                        print("[CLEANUP] Starting scheduled purge of expired accounts...")
                        with SessionLocal() as db:
                            from .services.user_service import UserService
                            user_service = UserService(db)
                            user_service.purge_deleted_users(settings.deletion_grace_period_days)
                    except Exception as e:
                        print(f"[ERROR] Soft-delete cleanup task failed: {e}")
                    
                    # Run once every 24 hours
                    await asyncio.sleep(24 * 3600)
            
            asyncio.create_task(purge_task_loop())
            print("[OK] Soft-delete cleanup task scheduled (runs every 24h)")
            
        except Exception as e:
            print(f"[ERROR] Database initialization failed: {e}")
            
        print("[OK] SoulSense API started successfully")
        print(f"[ENV] Environment: {settings.app_env}")
        print(f"[CONFIG] Debug mode: {settings.debug}")
        print(f"[DB] Database: {settings.database_url}")
        print(f"[API] API available at /api/v1")

    return app


app = create_app()
