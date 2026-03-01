import os
import psutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def check_memory_usage(threshold_mb: int = 512) -> bool:
    """
    Checks if the current process memory usage exceeds the threshold.
    Returns True if usage is within limits, False if it exceeds it.
    """
    process = psutil.Process(os.get_pid())
    mem_info = process.memory_info()
    mem_mb = mem_info.rss / (1024 * 1024)
    
    if mem_mb > threshold_mb:
        logger.warning(f"Proactive Memory Guard: Process {os.get_pid()} using {mem_mb:.2f} MB, exceeding threshold {threshold_mb} MB.")
        return False
    return True

def enforce_memory_limit(threshold_mb: int = 512):
    """
    If memory usage exceeds threshold, raises a MemoryError or signals for restart.
    In Celery, raising an exception can trigger a retry or worker termination if configured.
    """
    if not check_memory_usage(threshold_mb):
        # We can either raise an error or try to trigger a graceful exit
        # For Celery, raising an error is often enough to fail the task and let the worker continue (or restart if max-memory-per-child is set)
        raise MemoryError(f"Process memory threshold exceeded ({threshold_mb} MB)")

def get_total_system_memory_usage() -> float:
    """Returns system memory usage percentage."""
    return psutil.virtual_memory().percent
