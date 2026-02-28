import asyncio
import os
import logging
from typing import Dict, Any
from celery.exceptions import MaxRetriesExceededError
from api.celery_app import celery_app
from api.services.export_service_v2 import ExportServiceV2
from api.services.background_task_service import BackgroundTaskService, TaskStatus
from api.services.db_service import AsyncSessionLocal
from sqlalchemy import select
from api.models import User, NotificationLog
from api.services.data_archival_service import DataArchivalService
import redis
from api.config import get_settings_instance
from api.utils.distributed_lock import require_lock

logger = logging.getLogger(__name__)

def notify_user_via_ws(user_id: int, message: dict):
    settings = get_settings_instance()
    try:
        r = redis.from_url(settings.redis_url)
        payload = {
            "user_id": user_id,
            "payload": message
        }
        r.publish("soulsense_ws_events", json.dumps(payload))
    except Exception as e:
        logger.error(f"Failed to notify user via WS: {e}")

def run_async(coro):
    """Run an asynchronous coroutine from a synchronous context."""
    try:
        return asyncio.run(coro)
    except RuntimeError:
        # If there's an existing loop, use it
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)

@celery_app.task(bind=True, max_retries=3, acks_late=True, track_started=True)
def execute_async_export_task(self, job_id: str, user_id: int, username: str, format: str, options: Dict[str, Any]):
    """
    Celery task to generate an export asynchronously.
    Implements idempotent execution and exponential backoff retry.
    """
    try:
        run_async(_execute_async_export_db(job_id, user_id, username, format, options))
    except Exception as exc:
        logger.error(f"Task Failed for job {job_id}: {exc}")
        # Exponential backoff: 5, 25, 125 seconds
        backoff_delay = 5 ** (self.request.retries + 1)
        try:
            # Requeue task with exponential backoff
            self.retry(exc=exc, countdown=backoff_delay)
        except MaxRetriesExceededError:
            # DLQ behavior: mark as failed and log permanently
            logger.error(f"Max retries exceeded for job {job_id}. Sending to DLQ (marked as FAILED in DB).")
            run_async(_mark_task_failed(job_id, str(exc)))


@require_lock(name="job_{job_id}", timeout=60)
async def _execute_async_export_db(job_id: str, user_id: int, username: str, format: str, options: Dict[str, Any]):
    async with AsyncSessionLocal() as db:
        try:
            # Check for idempotency: if it's already completed
            task = await BackgroundTaskService.get_task(db, job_id)
            if task and task.status == TaskStatus.COMPLETED.value:
                logger.info(f"Task {job_id} already completed. Idempotent return.")
                return

            await BackgroundTaskService.update_task_status(db, job_id, TaskStatus.PROCESSING)
            
            stmt = select(User).filter(User.id == user_id)
            result = await db.execute(stmt)
            user = result.scalar_one_or_none()
            
            if not user:
                raise ValueError(f"User {user_id} not found")
            
            filepath, export_id = await ExportServiceV2.generate_export(
                db, user, format, options
            )
            
            result_data = {
                "filepath": filepath,
                "export_id": export_id,
                "format": format,
                "filename": os.path.basename(filepath),
                "download_url": f"/api/v1/reports/export/{export_id}/download"
            }
            
            await BackgroundTaskService.update_task_status(
                db, job_id, TaskStatus.COMPLETED, result=result_data
            )
            
            # Notify user via WebSocket
            notify_user_via_ws(user_id, {
                "type": "task_completed",
                "task_type": "export",
                "job_id": job_id,
                "message": "Your export has finished generating.",
                "download_url": result_data["download_url"],
                "filename": result_data["filename"]
            })
        except Exception as e:
            # Let the outer Celery task capture and retry
            raise e

async def _mark_task_failed(job_id: str, error_msg: str):
    async with AsyncSessionLocal() as db:
        await BackgroundTaskService.update_task_status(
            db, job_id, TaskStatus.FAILED, error_message=error_msg
        )

@celery_app.task(bind=True, max_retries=5, acks_late=True, track_started=True)
def send_notification_task(self, log_id: int, channel: str, user_id: int, content: Dict[str, str]):
    """
    Celery task to send a notification synchronously/asynchronously via worker.
    """
    try:
        run_async(_execute_send_notification(log_id, channel, user_id, content))
    except Exception as exc:
        logger.error(f"Notification Task Failed for log_id {log_id}: {exc}")
        backoff_delay = 5 ** (self.request.retries + 1)
        try:
            self.retry(exc=exc, countdown=backoff_delay)
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for notification {log_id}. Sent to DLQ.")
            run_async(_mark_notification_failed(log_id, str(exc)))

async def _execute_send_notification(log_id: int, channel: str, user_id: int, content: Dict[str, str]):
    from datetime import datetime, UTC
    async with AsyncSessionLocal() as db:
        stmt = select(NotificationLog).where(NotificationLog.id == log_id)
        res = await db.execute(stmt)
        log = res.scalar_one_or_none()
        
        if not log:
            return
            
        try:
            import asyncio
            # MOCK Simulate network latency
            await asyncio.sleep(1.0)
            
            # Implementation for actual dispatch goes here
            if channel == 'email':
                pass
            elif channel == 'push':
                pass
            elif channel == 'in_app':
                pass
                
            log.status = "sent"
            log.sent_at = datetime.now(UTC)
            
        except Exception as e:
            log.status = "failed"
            log.error_message = str(e)
            raise e
            
        finally:
            await db.commit()

async def _mark_notification_failed(log_id: int, error_msg: str):
    async with AsyncSessionLocal() as db:
        stmt = select(NotificationLog).where(NotificationLog.id == log_id)
        res = await db.execute(stmt)
        log = res.scalar_one_or_none()
        if log:
            log.status = "failed"
            log.error_message = error_msg
            await db.commit()

@celery_app.task(bind=True, max_retries=3, acks_late=True, track_started=True)
def generate_archive_task(self, job_id: str, user_id: int, password: str, include_pdf: bool, include_csv: bool, include_json: bool):
    """
    Celery task to generate a secure GDPR data archive asynchronously.
    """
    try:
        run_async(_execute_archive_generation(job_id, user_id, password, include_pdf, include_csv, include_json))
    except Exception as exc:
        logger.error(f"Archive Task Failed for job {job_id}: {exc}")
        backoff_delay = 5 ** (self.request.retries + 1)
        try:
            self.retry(exc=exc, countdown=backoff_delay)
        except MaxRetriesExceededError:
            logger.error(f"Max retries exceeded for archive {job_id}.")
            run_async(_mark_task_failed(job_id, str(exc)))

async def _execute_archive_generation(job_id: str, user_id: int, password: str, include_pdf: bool, include_csv: bool, include_json: bool):
    async with AsyncSessionLocal() as db:
        try:
            await BackgroundTaskService.update_task_status(db, job_id, TaskStatus.PROCESSING)
            
            stmt = select(User).where(User.id == user_id)
            result = await db.execute(stmt)
            user = result.scalar_one_or_none()
            if not user:
                raise ValueError(f"User {user_id} not found")
                
            filepath, export_id = await DataArchivalService.generate_comprehensive_archive(
                db=db,
                user=user,
                password=password,
                include_pdf=include_pdf,
                include_csv=include_csv,
                include_json=include_json
            )
            
            result_data = {
                "filepath": filepath,
                "export_id": export_id,
                "filename": os.path.basename(filepath),
                "download_url": f"/api/v1/archival/archive/{export_id}/download"
            }
            
            await BackgroundTaskService.update_task_status(
                db, job_id, TaskStatus.COMPLETED, result=result_data
            )
            
            # Notify user via WebSocket
            notify_user_via_ws(user_id, {
                "type": "task_completed",
                "task_type": "archival",
                "job_id": job_id,
                "message": "Your comprehensive GDPR data archive is ready.",
                "download_url": result_data["download_url"],
                "filename": result_data["filename"]
            })
        except Exception as e:
            await BackgroundTaskService.update_task_status(
                db, job_id, TaskStatus.FAILED, error_message=str(e)
            )
            raise e

