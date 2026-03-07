"""
A/B Experimentation Framework for Recommendations (#1442)

Provides a comprehensive framework for running A/B tests on recommendation algorithms,
including experiment management, user assignment, metric tracking, and statistical analysis.

Features:
- Experiment creation and lifecycle management
- Randomized user assignment to variants
- Variant configuration (control, treatment, multiple arms)
- Event tracking and metric collection
- Statistical significance calculation
- Automatic winner selection
- Gradual rollout support
- Real-time experiment monitoring

Example:
    from api.utils.ab_experiment_framework import ABExperimentFramework, ExperimentConfig
    
    framework = ABExperimentFramework()
    await framework.initialize()
    
    # Create experiment
    experiment = await framework.create_experiment(
        name="Recommendation Algorithm V2",
        config=ExperimentConfig(
            variants=[
                Variant(name="control", config={"algorithm": "collaborative_filtering"}),
                Variant(name="treatment", config={"algorithm": "neural_network"})
            ],
            traffic_split=[50, 50],
            primary_metric="click_through_rate"
        )
    )
    
    # Assign user to variant
    assignment = await framework.assign_user(
        experiment_id=experiment.experiment_id,
        user_id="user_123"
    )
"""

import asyncio
import hashlib
import json
import logging
import math
import random
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Callable
from uuid import uuid4

from scipy import stats
from sqlalchemy import text, Column, String, DateTime, Integer, Float, Boolean, Text, JSON
from sqlalchemy.ext.asyncio import AsyncSession, AsyncEngine
from sqlalchemy.orm import declarative_base

from ..services.db_service import AsyncSessionLocal, engine


logger = logging.getLogger("api.ab_experiment_framework")

Base = declarative_base()


class ExperimentStatus(str, Enum):
    """Status of an A/B experiment."""
    DRAFT = "draft"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ExperimentType(str, Enum):
    """Type of A/B experiment."""
    AB_TEST = "ab_test"  # Standard A/B test
    MULTI_ARMED_BANDIT = "multi_armed_bandit"  # Multi-armed bandit
    ROLLOUT = "rollout"  # Gradual rollout


class AssignmentMethod(str, Enum):
    """Method for user assignment to variants."""
    RANDOM = "random"  # Pure random assignment
    HASH = "hash"  # Consistent hashing by user_id
    STRATIFIED = "stratified"  # Stratified by user attributes


class MetricType(str, Enum):
    """Type of experiment metric."""
    CONVERSION = "conversion"  # Binary conversion
    COUNT = "count"  # Count metric
    CONTINUOUS = "continuous"  # Continuous value
    RATIO = "ratio"  # Ratio metric


@dataclass
class Variant:
    """Represents an experiment variant."""
    variant_id: str
    name: str
    description: Optional[str] = None
    config: Dict[str, Any] = field(default_factory=dict)
    traffic_percentage: float = 50.0
    is_control: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "name": self.name,
            "description": self.description,
            "config": self.config,
            "traffic_percentage": self.traffic_percentage,
            "is_control": self.is_control,
        }


@dataclass
class MetricConfig:
    """Configuration for an experiment metric."""
    metric_name: str
    metric_type: MetricType
    event_name: str  # Event to track
    is_primary: bool = False
    is_guardrail: bool = False  # Guardrail metric (must not regress)
    minimum_detectable_effect: float = 0.05  # MDE for power calculation
    direction: str = "increase"  # "increase" or "decrease" desired
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "metric_type": self.metric_type.value,
            "event_name": self.event_name,
            "is_primary": self.is_primary,
            "is_guardrail": self.is_guardrail,
            "minimum_detectable_effect": self.minimum_detectable_effect,
            "direction": self.direction,
        }


@dataclass
class ExperimentConfig:
    """Configuration for an A/B experiment."""
    variants: List[Variant]
    metrics: List[MetricConfig]
    traffic_split: List[float] = field(default_factory=lambda: [50.0, 50.0])
    assignment_method: AssignmentMethod = AssignmentMethod.HASH
    min_sample_size: int = 1000
    max_sample_size: Optional[int] = None
    confidence_level: float = 0.95
    runtime_days: int = 14
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "variants": [v.to_dict() for v in self.variants],
            "metrics": [m.to_dict() for m in self.metrics],
            "traffic_split": self.traffic_split,
            "assignment_method": self.assignment_method.value,
            "min_sample_size": self.min_sample_size,
            "max_sample_size": self.max_sample_size,
            "confidence_level": self.confidence_level,
            "runtime_days": self.runtime_days,
        }


@dataclass
class Experiment:
    """Represents an A/B experiment."""
    experiment_id: str
    name: str
    description: Optional[str]
    experiment_type: ExperimentType
    config: ExperimentConfig
    status: ExperimentStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    created_by: Optional[str] = None
    winner_variant_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "name": self.name,
            "description": self.description,
            "experiment_type": self.experiment_type.value,
            "config": self.config.to_dict(),
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "created_by": self.created_by,
            "winner_variant_id": self.winner_variant_id,
        }


@dataclass
class UserAssignment:
    """Represents a user's assignment to a variant."""
    assignment_id: str
    experiment_id: str
    user_id: str
    variant_id: str
    assigned_at: datetime
    attributes: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "assignment_id": self.assignment_id,
            "experiment_id": self.experiment_id,
            "user_id": self.user_id,
            "variant_id": self.variant_id,
            "assigned_at": self.assigned_at.isoformat(),
            "attributes": self.attributes,
        }


@dataclass
class ExperimentEvent:
    """Represents an experiment event (metric occurrence)."""
    event_id: str
    experiment_id: str
    user_id: str
    variant_id: str
    event_name: str
    event_value: float
    event_metadata: Dict[str, Any]
    timestamp: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "experiment_id": self.experiment_id,
            "user_id": self.user_id,
            "variant_id": self.variant_id,
            "event_name": self.event_name,
            "event_value": self.event_value,
            "event_metadata": self.event_metadata,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class VariantStats:
    """Statistics for a variant."""
    variant_id: str
    variant_name: str
    sample_size: int
    conversions: int = 0
    conversion_rate: float = 0.0
    total_value: float = 0.0
    mean_value: float = 0.0
    std_dev: float = 0.0
    confidence_interval_lower: float = 0.0
    confidence_interval_upper: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "variant_id": self.variant_id,
            "variant_name": self.variant_name,
            "sample_size": self.sample_size,
            "conversions": self.conversions,
            "conversion_rate": round(self.conversion_rate, 4),
            "total_value": round(self.total_value, 2),
            "mean_value": round(self.mean_value, 4),
            "std_dev": round(self.std_dev, 4),
            "confidence_interval": [
                round(self.confidence_interval_lower, 4),
                round(self.confidence_interval_upper, 4),
            ],
        }


@dataclass
class ExperimentResults:
    """Results of an A/B experiment."""
    experiment_id: str
    metric_name: str
    control_variant: VariantStats
    treatment_variants: List[VariantStats]
    p_value: float
    is_statistically_significant: bool
    relative_lift: float
    absolute_lift: float
    recommendation: str
    calculated_at: datetime
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "experiment_id": self.experiment_id,
            "metric_name": self.metric_name,
            "control_variant": self.control_variant.to_dict(),
            "treatment_variants": [v.to_dict() for v in self.treatment_variants],
            "p_value": round(self.p_value, 6),
            "is_statistically_significant": self.is_statistically_significant,
            "relative_lift": round(self.relative_lift, 4),
            "absolute_lift": round(self.absolute_lift, 4),
            "recommendation": self.recommendation,
            "calculated_at": self.calculated_at.isoformat(),
        }


class ABExperimentFramework:
    """
    Framework for managing A/B experiments on recommendations.
    
    Provides complete lifecycle management for experiments including:
    - Creation and configuration
    - User assignment to variants
    - Event tracking and metric collection
    - Statistical analysis and results
    - Winner selection and rollout
    
    Example:
        framework = ABExperimentFramework()
        await framework.initialize()
        
        # Create experiment
        exp = await framework.create_experiment(
            name="New Recommendation Algorithm",
            config=ExperimentConfig(...)
        )
        
        # Start experiment
        await framework.start_experiment(exp.experiment_id)
        
        # Assign users and track events
        assignment = await framework.assign_user(exp.experiment_id, "user_123")
        await framework.track_event(exp.experiment_id, "user_123", "click", 1.0)
        
        # Get results
        results = await framework.calculate_results(exp.experiment_id)
    """
    
    def __init__(self, engine: Optional[AsyncEngine] = None):
        self.engine = engine
        self._experiments: Dict[str, Experiment] = {}
        self._assignments: Dict[str, UserAssignment] = {}
        self._event_callbacks: List[Callable[[ExperimentEvent], None]] = []
    
    async def initialize(self) -> None:
        """Initialize the experiment framework."""
        await self._ensure_tables()
        logger.info("ABExperimentFramework initialized")
    
    async def _ensure_tables(self) -> None:
        """Ensure experiment tables exist."""
        if not self.engine:
            from ..services.db_service import engine as db_engine
            self.engine = db_engine
        
        async with self.engine.begin() as conn:
            # Experiments table
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ab_experiments (
                    id SERIAL PRIMARY KEY,
                    experiment_id VARCHAR(255) UNIQUE NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    experiment_type VARCHAR(50) DEFAULT 'ab_test',
                    config JSONB NOT NULL,
                    status VARCHAR(50) DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT NOW(),
                    started_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    created_by VARCHAR(255),
                    winner_variant_id VARCHAR(255),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            
            # User assignments table
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ab_user_assignments (
                    id SERIAL PRIMARY KEY,
                    assignment_id VARCHAR(255) UNIQUE NOT NULL,
                    experiment_id VARCHAR(255) NOT NULL,
                    user_id VARCHAR(255) NOT NULL,
                    variant_id VARCHAR(255) NOT NULL,
                    assigned_at TIMESTAMP DEFAULT NOW(),
                    attributes JSONB DEFAULT '{}',
                    UNIQUE(experiment_id, user_id)
                )
            """))
            
            # Events table
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ab_experiment_events (
                    id SERIAL PRIMARY KEY,
                    event_id VARCHAR(255) UNIQUE NOT NULL,
                    experiment_id VARCHAR(255) NOT NULL,
                    user_id VARCHAR(255) NOT NULL,
                    variant_id VARCHAR(255) NOT NULL,
                    event_name VARCHAR(255) NOT NULL,
                    event_value FLOAT DEFAULT 0,
                    event_metadata JSONB DEFAULT '{}',
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """))
            
            # Results table
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ab_experiment_results (
                    id SERIAL PRIMARY KEY,
                    result_id VARCHAR(255) UNIQUE NOT NULL,
                    experiment_id VARCHAR(255) NOT NULL,
                    metric_name VARCHAR(255) NOT NULL,
                    results JSONB NOT NULL,
                    calculated_at TIMESTAMP DEFAULT NOW()
                )
            """))
            
            # Create indexes
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_ab_exp_status 
                ON ab_experiments(status, created_at DESC)
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_ab_assignment_exp_user 
                ON ab_user_assignments(experiment_id, user_id)
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_ab_events_exp 
                ON ab_experiment_events(experiment_id, timestamp DESC)
            """))
            
            await conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_ab_events_user 
                ON ab_experiment_events(user_id, event_name)
            """))
        
        logger.info("A/B experiment tables ensured")
    
    async def create_experiment(
        self,
        name: str,
        config: ExperimentConfig,
        description: Optional[str] = None,
        experiment_type: ExperimentType = ExperimentType.AB_TEST,
        created_by: Optional[str] = None
    ) -> Experiment:
        """
        Create a new A/B experiment.
        
        Args:
            name: Experiment name
            config: Experiment configuration
            description: Optional description
            experiment_type: Type of experiment
            created_by: User who created the experiment
            
        Returns:
            Created Experiment
        """
        experiment_id = f"exp_{uuid4().hex[:12]}"
        
        # Ensure at least one variant is control
        has_control = any(v.is_control for v in config.variants)
        if not has_control and config.variants:
            config.variants[0].is_control = True
        
        # Generate variant IDs if not provided
        for variant in config.variants:
            if not variant.variant_id:
                variant.variant_id = f"var_{uuid4().hex[:8]}"
        
        experiment = Experiment(
            experiment_id=experiment_id,
            name=name,
            description=description,
            experiment_type=experiment_type,
            config=config,
            status=ExperimentStatus.DRAFT,
            created_at=datetime.utcnow(),
            created_by=created_by,
        )
        
        # Persist to database
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("""
                    INSERT INTO ab_experiments (
                        experiment_id, name, description, experiment_type,
                        config, status, created_at, created_by
                    ) VALUES (
                        :experiment_id, :name, :description, :experiment_type,
                        :config, :status, :created_at, :created_by
                    )
                """),
                {
                    "experiment_id": experiment.experiment_id,
                    "name": experiment.name,
                    "description": experiment.description,
                    "experiment_type": experiment.experiment_type.value,
                    "config": json.dumps(experiment.config.to_dict()),
                    "status": experiment.status.value,
                    "created_at": experiment.created_at,
                    "created_by": experiment.created_by,
                }
            )
            await session.commit()
        
        self._experiments[experiment_id] = experiment
        logger.info(f"Created experiment {experiment_id}: {name}")
        
        return experiment
    
    async def get_experiment(self, experiment_id: str) -> Optional[Experiment]:
        """Get an experiment by ID."""
        if experiment_id in self._experiments:
            return self._experiments[experiment_id]
        
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("SELECT * FROM ab_experiments WHERE experiment_id = :experiment_id"),
                {"experiment_id": experiment_id}
            )
            row = result.fetchone()
            
            if row:
                experiment = Experiment(
                    experiment_id=row.experiment_id,
                    name=row.name,
                    description=row.description,
                    experiment_type=ExperimentType(row.experiment_type),
                    config=ExperimentConfig(
                        variants=[Variant(**v) for v in row.config.get("variants", [])],
                        metrics=[MetricConfig(**m) for m in row.config.get("metrics", [])],
                        traffic_split=row.config.get("traffic_split", [50, 50]),
                        assignment_method=AssignmentMethod(row.config.get("assignment_method", "hash")),
                        min_sample_size=row.config.get("min_sample_size", 1000),
                        max_sample_size=row.config.get("max_sample_size"),
                        confidence_level=row.config.get("confidence_level", 0.95),
                        runtime_days=row.config.get("runtime_days", 14),
                    ),
                    status=ExperimentStatus(row.status),
                    created_at=row.created_at,
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    created_by=row.created_by,
                    winner_variant_id=row.winner_variant_id,
                )
                self._experiments[experiment_id] = experiment
                return experiment
        
        return None
    
    async def list_experiments(
        self,
        status: Optional[ExperimentStatus] = None,
        limit: int = 100
    ) -> List[Experiment]:
        """List experiments."""
        experiments = []
        
        async with AsyncSessionLocal() as session:
            if status:
                result = await session.execute(
                    text("""
                        SELECT * FROM ab_experiments
                        WHERE status = :status
                        ORDER BY created_at DESC
                        LIMIT :limit
                    """),
                    {"status": status.value, "limit": limit}
                )
            else:
                result = await session.execute(
                    text("""
                        SELECT * FROM ab_experiments
                        ORDER BY created_at DESC
                        LIMIT :limit
                    """),
                    {"limit": limit}
                )
            
            for row in result:
                exp = Experiment(
                    experiment_id=row.experiment_id,
                    name=row.name,
                    description=row.description,
                    experiment_type=ExperimentType(row.experiment_type),
                    config=ExperimentConfig(**row.config),
                    status=ExperimentStatus(row.status),
                    created_at=row.created_at,
                    started_at=row.started_at,
                    ended_at=row.ended_at,
                    created_by=row.created_by,
                    winner_variant_id=row.winner_variant_id,
                )
                experiments.append(exp)
                self._experiments[row.experiment_id] = exp
        
        return experiments
    
    async def start_experiment(self, experiment_id: str) -> bool:
        """Start a draft experiment."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    UPDATE ab_experiments
                    SET status = 'running', started_at = NOW(), updated_at = NOW()
                    WHERE experiment_id = :experiment_id AND status = 'draft'
                    RETURNING id
                """),
                {"experiment_id": experiment_id}
            )
            await session.commit()
            
            if result.fetchone():
                if experiment_id in self._experiments:
                    self._experiments[experiment_id].status = ExperimentStatus.RUNNING
                    self._experiments[experiment_id].started_at = datetime.utcnow()
                logger.info(f"Started experiment {experiment_id}")
                return True
        
        return False
    
    async def pause_experiment(self, experiment_id: str) -> bool:
        """Pause a running experiment."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    UPDATE ab_experiments
                    SET status = 'paused', updated_at = NOW()
                    WHERE experiment_id = :experiment_id AND status = 'running'
                    RETURNING id
                """),
                {"experiment_id": experiment_id}
            )
            await session.commit()
            
            if result.fetchone():
                if experiment_id in self._experiments:
                    self._experiments[experiment_id].status = ExperimentStatus.PAUSED
                logger.info(f"Paused experiment {experiment_id}")
                return True
        
        return False
    
    async def stop_experiment(
        self,
        experiment_id: str,
        winner_variant_id: Optional[str] = None
    ) -> bool:
        """Stop an experiment and optionally declare a winner."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    UPDATE ab_experiments
                    SET status = 'completed', ended_at = NOW(),
                        winner_variant_id = :winner_variant_id,
                        updated_at = NOW()
                    WHERE experiment_id = :experiment_id
                    AND status IN ('running', 'paused')
                    RETURNING id
                """),
                {"experiment_id": experiment_id, "winner_variant_id": winner_variant_id}
            )
            await session.commit()
            
            if result.fetchone():
                if experiment_id in self._experiments:
                    self._experiments[experiment_id].status = ExperimentStatus.COMPLETED
                    self._experiments[experiment_id].ended_at = datetime.utcnow()
                    self._experiments[experiment_id].winner_variant_id = winner_variant_id
                logger.info(f"Stopped experiment {experiment_id}, winner: {winner_variant_id}")
                return True
        
        return False
    
    async def assign_user(
        self,
        experiment_id: str,
        user_id: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Optional[UserAssignment]:
        """
        Assign a user to a variant.
        
        Args:
            experiment_id: Experiment ID
            user_id: User identifier
            attributes: Optional user attributes for stratification
            
        Returns:
            UserAssignment or None if experiment not running
        """
        experiment = await self.get_experiment(experiment_id)
        if not experiment or experiment.status != ExperimentStatus.RUNNING:
            return None
        
        # Check if user already assigned
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    SELECT * FROM ab_user_assignments
                    WHERE experiment_id = :experiment_id AND user_id = :user_id
                """),
                {"experiment_id": experiment_id, "user_id": user_id}
            )
            existing = result.fetchone()
            
            if existing:
                return UserAssignment(
                    assignment_id=existing.assignment_id,
                    experiment_id=existing.experiment_id,
                    user_id=existing.user_id,
                    variant_id=existing.variant_id,
                    assigned_at=existing.assigned_at,
                    attributes=existing.attributes,
                )
        
        # Assign to variant
        variant = self._assign_variant(experiment, user_id, attributes)
        if not variant:
            return None
        
        assignment_id = f"asn_{uuid4().hex[:12]}"
        assignment = UserAssignment(
            assignment_id=assignment_id,
            experiment_id=experiment_id,
            user_id=user_id,
            variant_id=variant.variant_id,
            assigned_at=datetime.utcnow(),
            attributes=attributes or {},
        )
        
        # Persist assignment
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("""
                    INSERT INTO ab_user_assignments (
                        assignment_id, experiment_id, user_id, variant_id,
                        assigned_at, attributes
                    ) VALUES (
                        :assignment_id, :experiment_id, :user_id, :variant_id,
                        :assigned_at, :attributes
                    )
                """),
                {
                    "assignment_id": assignment.assignment_id,
                    "experiment_id": assignment.experiment_id,
                    "user_id": assignment.user_id,
                    "variant_id": assignment.variant_id,
                    "assigned_at": assignment.assigned_at,
                    "attributes": json.dumps(assignment.attributes),
                }
            )
            await session.commit()
        
        self._assignments[assignment_id] = assignment
        logger.debug(f"Assigned user {user_id} to variant {variant.variant_id} in {experiment_id}")
        
        return assignment
    
    def _assign_variant(
        self,
        experiment: Experiment,
        user_id: str,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Optional[Variant]:
        """Assign user to variant based on assignment method."""
        variants = experiment.config.variants
        if not variants:
            return None
        
        method = experiment.config.assignment_method
        
        if method == AssignmentMethod.RANDOM:
            # Pure random assignment
            weights = [v.traffic_percentage for v in variants]
            return random.choices(variants, weights=weights, k=1)[0]
        
        elif method == AssignmentMethod.HASH:
            # Consistent hashing
            hash_input = f"{experiment.experiment_id}:{user_id}"
            hash_value = int(hashlib.md5(hash_input.encode()).hexdigest(), 16)
            
            # Normalize to 0-100 range
            bucket = hash_value % 100
            cumulative = 0
            
            for variant in variants:
                cumulative += variant.traffic_percentage
                if bucket < cumulative:
                    return variant
            
            return variants[-1]  # Fallback
        
        elif method == AssignmentMethod.STRATIFIED:
            # Stratified assignment (simplified)
            # In production, would use user attributes for stratification
            return self._assign_variant(experiment, user_id, None)  # Fallback to hash
        
        return variants[0]  # Default fallback
    
    async def track_event(
        self,
        experiment_id: str,
        user_id: str,
        event_name: str,
        event_value: float = 1.0,
        event_metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[ExperimentEvent]:
        """
        Track an experiment event.
        
        Args:
            experiment_id: Experiment ID
            user_id: User ID
            event_name: Name of the event
            event_value: Value of the event
            event_metadata: Additional metadata
            
        Returns:
            ExperimentEvent or None if user not assigned
        """
        # Get user's variant assignment
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    SELECT variant_id FROM ab_user_assignments
                    WHERE experiment_id = :experiment_id AND user_id = :user_id
                """),
                {"experiment_id": experiment_id, "user_id": user_id}
            )
            row = result.fetchone()
            
            if not row:
                logger.warning(f"User {user_id} not assigned to experiment {experiment_id}")
                return None
            
            variant_id = row.variant_id
        
        event_id = f"evt_{uuid4().hex[:12]}"
        event = ExperimentEvent(
            event_id=event_id,
            experiment_id=experiment_id,
            user_id=user_id,
            variant_id=variant_id,
            event_name=event_name,
            event_value=event_value,
            event_metadata=event_metadata or {},
            timestamp=datetime.utcnow(),
        )
        
        # Persist event
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("""
                    INSERT INTO ab_experiment_events (
                        event_id, experiment_id, user_id, variant_id,
                        event_name, event_value, event_metadata, timestamp
                    ) VALUES (
                        :event_id, :experiment_id, :user_id, :variant_id,
                        :event_name, :event_value, :event_metadata, :timestamp
                    )
                """),
                {
                    "event_id": event.event_id,
                    "experiment_id": event.experiment_id,
                    "user_id": event.user_id,
                    "variant_id": event.variant_id,
                    "event_name": event.event_name,
                    "event_value": event.event_value,
                    "event_metadata": json.dumps(event.event_metadata),
                    "timestamp": event.timestamp,
                }
            )
            await session.commit()
        
        # Trigger callbacks
        for callback in self._event_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(event)
                else:
                    callback(event)
            except Exception as e:
                logger.error(f"Event callback failed: {e}")
        
        return event
    
    async def calculate_results(
        self,
        experiment_id: str,
        metric_name: Optional[str] = None
    ) -> Optional[ExperimentResults]:
        """
        Calculate experiment results for a metric.
        
        Args:
            experiment_id: Experiment ID
            metric_name: Metric to analyze (defaults to primary metric)
            
        Returns:
            ExperimentResults or None
        """
        experiment = await self.get_experiment(experiment_id)
        if not experiment:
            return None
        
        # Get metric config
        if metric_name:
            metric_config = next(
                (m for m in experiment.config.metrics if m.metric_name == metric_name),
                None
            )
        else:
            # Use primary metric
            metric_config = next(
                (m for m in experiment.config.metrics if m.is_primary),
                experiment.config.metrics[0] if experiment.config.metrics else None
            )
        
        if not metric_config:
            return None
        
        metric_name = metric_config.metric_name
        
        # Get variant stats
        async with AsyncSessionLocal() as session:
            # Get control variant
            control_variant = next(
                (v for v in experiment.config.variants if v.is_control),
                experiment.config.variants[0] if experiment.config.variants else None
            )
            
            if not control_variant:
                return None
            
            # Calculate stats for each variant
            variant_stats = []
            for variant in experiment.config.variants:
                stats = await self._calculate_variant_stats(
                    session, experiment_id, variant, metric_config
                )
                variant_stats.append(stats)
            
            control_stats = next(
                (s for s in variant_stats if s.variant_id == control_variant.variant_id),
                None
            )
            treatment_stats = [s for s in variant_stats if s.variant_id != control_variant.variant_id]
            
            if not control_stats or not treatment_stats:
                return None
            
            # Calculate statistical significance
            # For simplicity, using two-proportion z-test for conversion metrics
            # In production, would use appropriate test based on metric type
            
            treatment = treatment_stats[0]  # Use first treatment for comparison
            
            # Calculate p-value using z-test
            p_value = self._calculate_p_value(control_stats, treatment, metric_config)
            
            is_significant = p_value < (1 - experiment.config.confidence_level)
            
            # Calculate lift
            if control_stats.conversion_rate > 0:
                relative_lift = (treatment.conversion_rate - control_stats.conversion_rate) / control_stats.conversion_rate
            else:
                relative_lift = 0.0
            
            absolute_lift = treatment.conversion_rate - control_stats.conversion_rate
            
            # Generate recommendation
            if is_significant:
                if metric_config.direction == "increase" and relative_lift > 0:
                    recommendation = f"Treatment shows significant improvement ({relative_lift:.1%} lift). Consider rolling out."
                elif metric_config.direction == "decrease" and relative_lift < 0:
                    recommendation = f"Treatment shows significant improvement ({abs(relative_lift):.1%} reduction). Consider rolling out."
                else:
                    recommendation = "Treatment shows significant change but in undesired direction. Do not roll out."
            else:
                recommendation = "No statistically significant difference detected. Continue running or stop experiment."
            
            results = ExperimentResults(
                experiment_id=experiment_id,
                metric_name=metric_name,
                control_variant=control_stats,
                treatment_variants=treatment_stats,
                p_value=p_value,
                is_statistically_significant=is_significant,
                relative_lift=relative_lift,
                absolute_lift=absolute_lift,
                recommendation=recommendation,
                calculated_at=datetime.utcnow(),
            )
            
            # Persist results
            await session.execute(
                text("""
                    INSERT INTO ab_experiment_results (
                        result_id, experiment_id, metric_name, results, calculated_at
                    ) VALUES (
                        :result_id, :experiment_id, :metric_name, :results, NOW()
                    )
                """),
                {
                    "result_id": f"res_{uuid4().hex[:12]}",
                    "experiment_id": experiment_id,
                    "metric_name": metric_name,
                    "results": json.dumps(results.to_dict()),
                }
            )
            await session.commit()
            
            return results
    
    async def _calculate_variant_stats(
        self,
        session: AsyncSession,
        experiment_id: str,
        variant: Variant,
        metric_config: MetricConfig
    ) -> VariantStats:
        """Calculate statistics for a variant."""
        # Get sample size (number of users assigned)
        result = await session.execute(
            text("""
                SELECT COUNT(*) as count FROM ab_user_assignments
                WHERE experiment_id = :experiment_id AND variant_id = :variant_id
            """),
            {"experiment_id": experiment_id, "variant_id": variant.variant_id}
        )
        sample_size = result.scalar() or 0
        
        # Get event stats
        result = await session.execute(
            text("""
                SELECT 
                    COUNT(*) as event_count,
                    SUM(event_value) as total_value,
                    AVG(event_value) as avg_value,
                    STDDEV(event_value) as std_dev
                FROM ab_experiment_events
                WHERE experiment_id = :experiment_id
                AND variant_id = :variant_id
                AND event_name = :event_name
            """),
            {
                "experiment_id": experiment_id,
                "variant_id": variant.variant_id,
                "event_name": metric_config.event_name,
            }
        )
        row = result.fetchone()
        
        event_count = row.event_count or 0
        total_value = row.total_value or 0.0
        mean_value = row.avg_value or 0.0
        std_dev = row.std_dev or 0.0
        
        # Calculate conversion rate (users with events / total users)
        conversion_rate = event_count / sample_size if sample_size > 0 else 0.0
        
        # Calculate confidence interval (95%)
        if sample_size > 1:
            se = std_dev / math.sqrt(sample_size)
            ci_lower = mean_value - 1.96 * se
            ci_upper = mean_value + 1.96 * se
        else:
            ci_lower = ci_upper = 0.0
        
        return VariantStats(
            variant_id=variant.variant_id,
            variant_name=variant.name,
            sample_size=sample_size,
            conversions=event_count,
            conversion_rate=conversion_rate,
            total_value=total_value,
            mean_value=mean_value,
            std_dev=std_dev,
            confidence_interval_lower=ci_lower,
            confidence_interval_upper=ci_upper,
        )
    
    def _calculate_p_value(
        self,
        control: VariantStats,
        treatment: VariantStats,
        metric_config: MetricConfig
    ) -> float:
        """Calculate p-value using two-proportion z-test."""
        if control.sample_size == 0 or treatment.sample_size == 0:
            return 1.0
        
        # Convert to proportions
        p1 = control.conversion_rate
        p2 = treatment.conversion_rate
        n1 = control.sample_size
        n2 = treatment.sample_size
        
        # Pooled proportion
        p_pool = (control.conversions + treatment.conversions) / (n1 + n2)
        
        # Standard error
        se = math.sqrt(p_pool * (1 - p_pool) * (1/n1 + 1/n2))
        
        if se == 0:
            return 1.0
        
        # Z-score
        z = (p2 - p1) / se
        
        # Two-tailed p-value
        p_value = 2 * (1 - stats.norm.cdf(abs(z)))
        
        return p_value
    
    async def get_experiment_stats(self, experiment_id: str) -> Dict[str, Any]:
        """Get comprehensive experiment statistics."""
        experiment = await self.get_experiment(experiment_id)
        if not experiment:
            return {}
        
        async with AsyncSessionLocal() as session:
            # Total assignments
            result = await session.execute(
                text("""
                    SELECT COUNT(*) FROM ab_user_assignments
                    WHERE experiment_id = :experiment_id
                """),
                {"experiment_id": experiment_id}
            )
            total_assignments = result.scalar() or 0
            
            # Assignments by variant
            result = await session.execute(
                text("""
                    SELECT variant_id, COUNT(*) as count
                    FROM ab_user_assignments
                    WHERE experiment_id = :experiment_id
                    GROUP BY variant_id
                """),
                {"experiment_id": experiment_id}
            )
            assignments_by_variant = {r.variant_id: r.count for r in result}
            
            # Total events
            result = await session.execute(
                text("""
                    SELECT COUNT(*) FROM ab_experiment_events
                    WHERE experiment_id = :experiment_id
                """),
                {"experiment_id": experiment_id}
            )
            total_events = result.scalar() or 0
            
            # Events by metric
            result = await session.execute(
                text("""
                    SELECT event_name, COUNT(*) as count
                    FROM ab_experiment_events
                    WHERE experiment_id = :experiment_id
                    GROUP BY event_name
                """),
                {"experiment_id": experiment_id}
            )
            events_by_name = {r.event_name: r.count for r in result}
        
        return {
            "experiment_id": experiment_id,
            "status": experiment.status.value,
            "total_assignments": total_assignments,
            "assignments_by_variant": assignments_by_variant,
            "total_events": total_events,
            "events_by_name": events_by_name,
            "days_running": (datetime.utcnow() - experiment.started_at).days if experiment.started_at else 0,
        }
    
    async def get_running_experiments(self) -> List[Experiment]:
        """Get all currently running experiments."""
        return await self.list_experiments(status=ExperimentStatus.RUNNING)
    
    def register_event_callback(
        self,
        callback: Callable[[ExperimentEvent], None]
    ) -> None:
        """Register a callback for experiment events."""
        self._event_callbacks.append(callback)
    
    async def check_experiment_health(self) -> Dict[str, Any]:
        """Check health of the experiment framework."""
        async with AsyncSessionLocal() as session:
            # Count experiments by status
            result = await session.execute(
                text("""
                    SELECT status, COUNT(*) as count
                    FROM ab_experiments
                    GROUP BY status
                """)
            )
            status_counts = {r.status: r.count for r in result}
            
            # Total assignments
            result = await session.execute(
                text("SELECT COUNT(*) FROM ab_user_assignments")
            )
            total_assignments = result.scalar() or 0
            
            # Total events
            result = await session.execute(
                text("SELECT COUNT(*) FROM ab_experiment_events")
            )
            total_events = result.scalar() or 0
        
        return {
            "status": "healthy",
            "experiments_by_status": status_counts,
            "total_assignments": total_assignments,
            "total_events": total_events,
            "timestamp": datetime.utcnow().isoformat(),
        }


# Global instance
_experiment_framework: Optional[ABExperimentFramework] = None


async def get_experiment_framework(
    engine: Optional[AsyncEngine] = None
) -> ABExperimentFramework:
    """Get or create the global experiment framework."""
    global _experiment_framework
    
    if _experiment_framework is None:
        _experiment_framework = ABExperimentFramework(engine)
        await _experiment_framework.initialize()
    
    return _experiment_framework
