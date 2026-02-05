from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from datetime import datetime
import os
import logging
from typing import Dict

from ..services.db_service import get_db
from ..services.export_service import ExportService
# Use root models to avoid import conflicts
from ..root_models import User
from .auth import get_current_user

router = APIRouter()
logger = logging.getLogger(__name__)

# Simple in-memory rate limiter: {user_id: timestamp}
_last_export_request: Dict[int, datetime] = {}
RATE_LIMIT_SECONDS = 60

# Simple in-memory job status: {job_id: {"status": str, "filename": str, "created_at": datetime}}
_export_jobs: Dict[str, dict] = {}

@router.post("")
async def generate_export(
    request: dict, 
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Generate an export of the user's exam data.
    
    Payload: {"format": "json" | "csv"}
    """
    export_format = request.get("format", "json").lower()
    
    # Rate Limiting
    last_req = _last_export_request.get(current_user.id)
    if last_req:
        elapsed = (datetime.now() - last_req).total_seconds()
        if elapsed < RATE_LIMIT_SECONDS:
            raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Try again in {int(RATE_LIMIT_SECONDS - elapsed)} seconds.")
            
    try:
        # Generate Export
        filepath, job_id = ExportService.generate_export(db, current_user, export_format)
        
        # Update Rate Limit
        _last_export_request[current_user.id] = datetime.now()
        
        filename = os.path.basename(filepath)
        
        # Store Job Status
        _export_jobs[job_id] = {
            "status": "completed",
            "filename": filename,
            "created_at": datetime.now(),
            "user_id": current_user.id
        }
        
        return {
            "job_id": job_id,
            "status": "completed",
            "filename": filename,
            "download_url": f"/api/v1/export/{filename}/download"
        }
        
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Export failed for {current_user.username}: {e}")
        # Standard generic error for security
        raise HTTPException(status_code=500, detail="Failed to generate export.")

@router.get("/{job_id}/status")
async def get_export_status(
    job_id: str,
    current_user: User = Depends(get_current_user)
):
    """
    Get the status of an export job.
    """
    job = _export_jobs.get(job_id)
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
        
    if job["user_id"] != current_user.id:
        raise HTTPException(status_code=403, detail="Access denied.")
        
    return {
        "job_id": job_id,
        "status": job["status"],
        "filename": job["filename"],
        "download_url": f"/api/v1/export/{job['filename']}/download"
    }

@router.get("/{filename}/download")
async def download_export(
    filename: str,
    current_user: User = Depends(get_current_user)
):
    """
    Download a generated export file.
    Validates ownership before serving.
    """
    # 1. Access Control
    if not ExportService.validate_export_access(current_user, filename):
        # Return 404 to avoid leaking existence of other files (or 403 explicit)
        # 403 is better for "I know who you are but no".
        raise HTTPException(status_code=403, detail="Access denied to this export file.")
        
    # 2. Path Resolution & Existence
    try:
        # Re-use strict logic from service or utils (service wrapper ensures valid base dir)
        # We manually construct to check existence first
        file_path = ExportService.EXPORT_DIR / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="Export file not found or expired.")
            
        # 3. Serve File
        return FileResponse(
            path=file_path, 
            filename=filename, 
            media_type='application/octet-stream'
        )
        
    except Exception as e:
        logger.error(f"Download failed for {filename}: {e}")
        raise HTTPException(status_code=404, detail="File could not be accessed.")
