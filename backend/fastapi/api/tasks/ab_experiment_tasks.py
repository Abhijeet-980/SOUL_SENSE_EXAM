"""
A/B Experiment Celery Tasks (#1442)

Background tasks for A/B experiment operations including:
- Experiment health monitoring
- Automatic results calculation
- Daily/weekly reporting
- Data cleanup
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from celery import shared_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from ..utils.ab_experiment_framework import (
    get_experiment_framework,
    ABExperimentFramework,
    ExperimentStatus,
    ExperimentType,
)
from ..services.db_service import engine


logger = logging.getLogger("api.tasks.ab_experiment")


# --- Experiment Monitoring Tasks ---

@shared_task
def check_running_experiments() -> Dict[str, Any]:
    """
    Check health and progress of running experiments.
    
    Returns:
        Monitoring summary
    """
    logger.info("Checking running experiments")
    
    async def _check():
        framework = await get_experiment_framework(engine)
        experiments = await framework.get_running_experiments()
        
        summaries = []
        
        for exp in experiments:
            stats = await framework.get_experiment_stats(exp.experiment_id)
            
            # Calculate days running
            days_running = stats.get("days_running", 0)
            total_assignments = stats.get("total_assignments", 0)
            min_sample = exp.config.min_sample_size
            
            # Determine status
            status = "on_track"
            issues = []
            
            if total_assignments < min_sample and days_running > exp.config.runtime_days / 2:
                status = "low_traffic"
                issues.append(f"Low traffic: {total_assignments}/{min_sample} samples")
            
            if days_running >= exp.config.runtime_days and total_assignments >= min_sample:
                status = "ready_for_analysis"
                issues.append("Experiment has reached target runtime and sample size")
            
            summaries.append({
                "experiment_id": exp.experiment_id,
                "name": exp.name,
                "days_running": days_running,
                "total_assignments": total_assignments,
                "target_sample_size": min_sample,
                "target_days": exp.config.runtime_days,
                "status": status,
                "issues": issues,
            })
        
        return {
            "status": "completed",
            "running_experiments": len(experiments),
            "summaries": summaries,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_check())
    except Exception as exc:
        logger.error(f"Experiment check failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


@shared_task
def auto_calculate_experiment_results() -> Dict[str, Any]:
    """
    Automatically calculate results for experiments that have sufficient data.
    
    Returns:
        Calculation summary
    """
    logger.info("Auto-calculating experiment results")
    
    async def _calculate():
        framework = await get_experiment_framework(engine)
        experiments = await framework.get_running_experiments()
        
        calculated = []
        
        for exp in experiments:
            stats = await framework.get_experiment_stats(exp.experiment_id)
            
            # Only calculate if we have enough samples
            if stats.get("total_assignments", 0) >= exp.config.min_sample_size:
                try:
                    for metric in exp.config.metrics:
                        results = await framework.calculate_results(
                            exp.experiment_id,
                            metric.metric_name
                        )
                        
                        if results:
                            calculated.append({
                                "experiment_id": exp.experiment_id,
                                "experiment_name": exp.name,
                                "metric": metric.metric_name,
                                "is_significant": results.is_statistically_significant,
                                "relative_lift": results.relative_lift,
                                "p_value": results.p_value,
                            })
                            
                            # Log significant results
                            if results.is_statistically_significant:
                                logger.info(
                                    f"Experiment {exp.experiment_id} ({exp.name}) shows "
                                    f"significant results for {metric.metric_name}: "
                                    f"{results.relative_lift:.1%} lift (p={results.p_value:.4f})"
                                )
                
                except Exception as e:
                    logger.error(f"Failed to calculate results for {exp.experiment_id}: {e}")
        
        return {
            "status": "completed",
            "experiments_calculated": len(calculated),
            "results": calculated,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_calculate())
    except Exception as exc:
        logger.error(f"Auto calculation failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Reporting Tasks ---

@shared_task
def generate_daily_experiment_report() -> Dict[str, Any]:
    """
    Generate daily experiment report.
    
    Returns:
        Report data
    """
    logger.info("Generating daily experiment report")
    
    async def _generate():
        framework = await get_experiment_framework(engine)
        
        # Get all experiments from last 30 days
        all_experiments = await framework.list_experiments(limit=1000)
        recent_experiments = [
            e for e in all_experiments
            if e.created_at > datetime.utcnow() - timedelta(days=30)
        ]
        
        # Categorize
        running = [e for e in recent_experiments if e.status == ExperimentStatus.RUNNING]
        completed = [e for e in recent_experiments if e.status == ExperimentStatus.COMPLETED]
        
        # Get stats for running experiments
        running_stats = []
        for exp in running[:5]:  # Top 5
            stats = await framework.get_experiment_stats(exp.experiment_id)
            running_stats.append({
                "experiment_id": exp.experiment_id,
                "name": exp.name,
                "assignments": stats.get("total_assignments", 0),
                "events": stats.get("total_events", 0),
            })
        
        # Summary
        report = {
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "summary": {
                "total_recent_experiments": len(recent_experiments),
                "running": len(running),
                "completed": len(completed),
            },
            "running_experiments": running_stats,
            "recommendations": [],
        }
        
        # Add recommendations
        for exp in running:
            stats = await framework.get_experiment_stats(exp.experiment_id)
            if stats.get("days_running", 0) >= exp.config.runtime_days:
                if stats.get("total_assignments", 0) >= exp.config.min_sample_size:
                    report["recommendations"].append({
                        "experiment_id": exp.experiment_id,
                        "name": exp.name,
                        "recommendation": "Consider stopping - reached target duration and sample size",
                    })
        
        return report
    
    try:
        return asyncio.run(_generate())
    except Exception as exc:
        logger.error(f"Report generation failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


@shared_task
def generate_experiment_summary(experiment_id: str) -> Dict[str, Any]:
    """
    Generate comprehensive summary for a specific experiment.
    
    Args:
        experiment_id: Experiment ID
        
    Returns:
        Experiment summary
    """
    logger.info(f"Generating summary for experiment {experiment_id}")
    
    async def _generate():
        framework = await get_experiment_framework(engine)
        
        experiment = await framework.get_experiment(experiment_id)
        if not experiment:
            return {"status": "not_found", "experiment_id": experiment_id}
        
        stats = await framework.get_experiment_stats(experiment_id)
        
        # Get results for all metrics
        results_by_metric = {}
        for metric in experiment.config.metrics:
            try:
                results = await framework.calculate_results(experiment_id, metric.metric_name)
                if results:
                    results_by_metric[metric.metric_name] = results.to_dict()
            except Exception as e:
                logger.warning(f"Could not calculate results for {metric.metric_name}: {e}")
        
        return {
            "status": "completed",
            "experiment": experiment.to_dict(),
            "statistics": stats,
            "results_by_metric": results_by_metric,
            "generated_at": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_generate())
    except Exception as exc:
        logger.error(f"Summary generation failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Data Cleanup Tasks ---

@shared_task
def cleanup_old_experiment_data(retention_days: int = 90) -> Dict[str, Any]:
    """
    Clean up old experiment data.
    
    Args:
        retention_days: Days to retain data
        
    Returns:
        Cleanup summary
    """
    logger.info(f"Cleaning up experiment data older than {retention_days} days")
    
    async def _cleanup():
        from sqlalchemy import text
        from ..services.db_service import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            # Clean up old events
            result = await session.execute(
                text("""
                    DELETE FROM ab_experiment_events
                    WHERE timestamp < NOW() - INTERVAL ':days days'
                    RETURNING COUNT(*)
                """),
                {"days": retention_days}
            )
            events_deleted = result.scalar()
            
            # Clean up old assignments for completed experiments
            result = await session.execute(
                text("""
                    DELETE FROM ab_user_assignments
                    WHERE assigned_at < NOW() - INTERVAL ':days days'
                    AND experiment_id IN (
                        SELECT experiment_id FROM ab_experiments
                        WHERE status IN ('completed', 'cancelled')
                    )
                    RETURNING COUNT(*)
                """),
                {"days": retention_days}
            )
            assignments_deleted = result.scalar()
            
            await session.commit()
        
        return {
            "status": "completed",
            "events_deleted": events_deleted,
            "assignments_deleted": assignments_deleted,
            "retention_days": retention_days,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_cleanup())
    except Exception as exc:
        logger.error(f"Cleanup failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Automated Actions ---

@shared_task
def auto_stop_completed_experiments() -> Dict[str, Any]:
    """
    Automatically stop experiments that have reached their target duration.
    
    Returns:
        Action summary
    """
    logger.info("Checking for experiments to auto-stop")
    
    async def _check():
        framework = await get_experiment_framework(engine)
        experiments = await framework.get_running_experiments()
        
        stopped = []
        
        for exp in experiments:
            stats = await framework.get_experiment_stats(exp.experiment_id)
            
            days_running = stats.get("days_running", 0)
            total_assignments = stats.get("total_assignments", 0)
            
            # Stop if exceeded runtime and has enough samples
            if days_running > exp.config.runtime_days * 1.5:  # 50% grace period
                if total_assignments >= exp.config.min_sample_size:
                    try:
                        # Calculate results to determine winner
                        results = await framework.calculate_results(exp.experiment_id)
                        winner_id = None
                        
                        if results and results.is_statistically_significant:
                            # Pick treatment with best lift
                            best_treatment = max(
                                results.treatment_variants,
                                key=lambda v: v.conversion_rate
                            )
                            winner_id = best_treatment.variant_id
                        
                        await framework.stop_experiment(exp.experiment_id, winner_id)
                        
                        stopped.append({
                            "experiment_id": exp.experiment_id,
                            "name": exp.name,
                            "winner_variant_id": winner_id,
                            "reason": "Exceeded target runtime",
                        })
                        
                        logger.info(f"Auto-stopped experiment {exp.experiment_id}")
                    
                    except Exception as e:
                        logger.error(f"Failed to auto-stop {exp.experiment_id}: {e}")
        
        return {
            "status": "completed",
            "experiments_stopped": len(stopped),
            "stopped": stopped,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_check())
    except Exception as exc:
        logger.error(f"Auto-stop check failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Export Tasks ---

@shared_task
def export_experiment_data(experiment_id: str, format: str = "json") -> Dict[str, Any]:
    """
    Export experiment data for external analysis.
    
    Args:
        experiment_id: Experiment ID
        format: Export format (json, csv)
        
    Returns:
        Export result
    """
    logger.info(f"Exporting data for experiment {experiment_id}")
    
    async def _export():
        from sqlalchemy import text
        from ..services.db_service import AsyncSessionLocal
        
        async with AsyncSessionLocal() as session:
            # Get assignments
            result = await session.execute(
                text("""
                    SELECT user_id, variant_id, assigned_at, attributes
                    FROM ab_user_assignments
                    WHERE experiment_id = :experiment_id
                """),
                {"experiment_id": experiment_id}
            )
            assignments = [
                {
                    "user_id": r.user_id,
                    "variant_id": r.variant_id,
                    "assigned_at": r.assigned_at.isoformat(),
                    "attributes": r.attributes,
                }
                for r in result
            ]
            
            # Get events
            result = await session.execute(
                text("""
                    SELECT user_id, variant_id, event_name, event_value, timestamp
                    FROM ab_experiment_events
                    WHERE experiment_id = :experiment_id
                """),
                {"experiment_id": experiment_id}
            )
            events = [
                {
                    "user_id": r.user_id,
                    "variant_id": r.variant_id,
                    "event_name": r.event_name,
                    "event_value": r.event_value,
                    "timestamp": r.timestamp.isoformat(),
                }
                for r in result
            ]
        
        data = {
            "experiment_id": experiment_id,
            "exported_at": datetime.utcnow().isoformat(),
            "assignments_count": len(assignments),
            "events_count": len(events),
            "assignments": assignments[:1000],  # Limit for response
            "events": events[:1000],
        }
        
        # In production, would upload to S3 and return URL
        return {
            "status": "completed",
            "format": format,
            "data_summary": {
                "assignments": len(assignments),
                "events": len(events),
            },
            "preview": data,
        }
    
    try:
        return asyncio.run(_export())
    except Exception as exc:
        logger.error(f"Export failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Notification Tasks ---

@shared_task
def notify_experiment_milestones() -> Dict[str, Any]:
    """
    Check for and notify on experiment milestones.
    
    Returns:
        Notification summary
    """
    logger.info("Checking for experiment milestones")
    
    async def _check():
        framework = await get_experiment_framework(engine)
        experiments = await framework.get_running_experiments()
        
        milestones = []
        
        for exp in experiments:
            stats = await framework.get_experiment_stats(exp.experiment_id)
            
            days_running = stats.get("days_running", 0)
            total_assignments = stats.get("total_assignments", 0)
            target_days = exp.config.runtime_days
            target_sample = exp.config.min_sample_size
            
            # Check milestones
            if days_running == target_days // 2:
                milestones.append({
                    "experiment_id": exp.experiment_id,
                    "name": exp.name,
                    "milestone": "50% of target duration",
                    "message": f"Experiment is at 50% of target duration ({days_running}/{target_days} days)",
                })
            
            if total_assignments >= target_sample and days_running < target_days:
                milestones.append({
                    "experiment_id": exp.experiment_id,
                    "name": exp.name,
                    "milestone": "Target sample size reached",
                    "message": f"Experiment has reached target sample size ({total_assignments}/{target_sample})",
                })
            
            # Check for significant results
            try:
                results = await framework.calculate_results(exp.experiment_id)
                if results and results.is_statistically_significant:
                    milestones.append({
                        "experiment_id": exp.experiment_id,
                        "name": exp.name,
                        "milestone": "Statistically significant result",
                        "message": f"Experiment shows {results.relative_lift:.1%} lift with p={results.p_value:.4f}",
                    })
            except Exception:
                pass  # Not enough data yet
        
        # In production, would send actual notifications
        for m in milestones:
            logger.info(f"Milestone: {m['milestone']} - {m['name']}")
        
        return {
            "status": "completed",
            "milestones_found": len(milestones),
            "milestones": milestones,
            "timestamp": datetime.utcnow().isoformat(),
        }
    
    try:
        return asyncio.run(_check())
    except Exception as exc:
        logger.error(f"Milestone check failed: {exc}")
        return {
            "status": "failed",
            "error": str(exc),
        }


# --- Setup Task ---

@shared_task
def setup_experiment_schedules() -> Dict[str, Any]:
    """
    Setup periodic experiment maintenance schedules.
    
    Returns:
        Setup result
    """
    logger.info("Setting up experiment schedules")
    
    return {
        "status": "configured",
        "schedules": [
            {
                "task": "api.tasks.ab_experiment_tasks.check_running_experiments",
                "schedule": "hourly",
            },
            {
                "task": "api.tasks.ab_experiment_tasks.auto_calculate_experiment_results",
                "schedule": "every 6 hours",
            },
            {
                "task": "api.tasks.ab_experiment_tasks.generate_daily_experiment_report",
                "schedule": "daily at 9:00",
            },
            {
                "task": "api.tasks.ab_experiment_tasks.auto_stop_completed_experiments",
                "schedule": "daily at 2:00",
            },
            {
                "task": "api.tasks.ab_experiment_tasks.notify_experiment_milestones",
                "schedule": "every 12 hours",
            },
            {
                "task": "api.tasks.ab_experiment_tasks.cleanup_old_experiment_data",
                "schedule": "weekly",
            },
        ],
        "timestamp": datetime.utcnow().isoformat(),
    }
