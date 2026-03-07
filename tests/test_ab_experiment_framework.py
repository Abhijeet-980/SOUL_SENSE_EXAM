"""
Tests for A/B Experimentation Framework (#1442)

Comprehensive tests for A/B experiment management, user assignment,
event tracking, and statistical analysis.
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from typing import List, Dict, Any
from unittest.mock import Mock, patch, AsyncMock

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base

# Import the module under test
import sys
sys.path.insert(0, 'backend/fastapi')

from api.utils.ab_experiment_framework import (
    ABExperimentFramework,
    Experiment,
    ExperimentConfig,
    ExperimentStatus,
    ExperimentType,
    AssignmentMethod,
    MetricType,
    Variant,
    MetricConfig,
    UserAssignment,
    ExperimentEvent,
    VariantStats,
    ExperimentResults,
    get_experiment_framework,
)


Base = declarative_base()


# Test Fixtures

@pytest.fixture
async def async_engine():
    """Create test async engine."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def framework(async_engine):
    """Create initialized experiment framework."""
    fw = ABExperimentFramework(async_engine)
    
    # Create tables
    async with async_engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ab_experiments (
                id INTEGER PRIMARY KEY,
                experiment_id TEXT UNIQUE NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                experiment_type TEXT DEFAULT 'ab_test',
                config TEXT NOT NULL,
                status TEXT DEFAULT 'draft',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                created_by TEXT,
                winner_variant_id TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ab_user_assignments (
                id INTEGER PRIMARY KEY,
                assignment_id TEXT UNIQUE NOT NULL,
                experiment_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                variant_id TEXT NOT NULL,
                assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                attributes TEXT DEFAULT '{}',
                UNIQUE(experiment_id, user_id)
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ab_experiment_events (
                id INTEGER PRIMARY KEY,
                event_id TEXT UNIQUE NOT NULL,
                experiment_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                variant_id TEXT NOT NULL,
                event_name TEXT NOT NULL,
                event_value REAL DEFAULT 0,
                event_metadata TEXT DEFAULT '{}',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ab_experiment_results (
                id INTEGER PRIMARY KEY,
                result_id TEXT UNIQUE NOT NULL,
                experiment_id TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                results TEXT NOT NULL,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
    
    await fw.initialize()
    yield fw


@pytest.fixture
def sample_variants():
    """Create sample variants."""
    return [
        Variant(
            variant_id="",
            name="control",
            description="Control variant",
            config={"algorithm": "v1"},
            traffic_percentage=50.0,
            is_control=True,
        ),
        Variant(
            variant_id="",
            name="treatment",
            description="Treatment variant",
            config={"algorithm": "v2"},
            traffic_percentage=50.0,
            is_control=False,
        ),
    ]


@pytest.fixture
def sample_metrics():
    """Create sample metrics."""
    return [
        MetricConfig(
            metric_name="click_through_rate",
            metric_type=MetricType.CONVERSION,
            event_name="click",
            is_primary=True,
            minimum_detectable_effect=0.05,
            direction="increase",
        ),
    ]


@pytest.fixture
def sample_config(sample_variants, sample_metrics):
    """Create sample experiment configuration."""
    return ExperimentConfig(
        variants=sample_variants,
        metrics=sample_metrics,
        traffic_split=[50.0, 50.0],
        assignment_method=AssignmentMethod.HASH,
        min_sample_size=100,
        runtime_days=14,
    )


# --- Test Classes ---

class TestVariant:
    """Test variant model."""
    
    def test_variant_creation(self):
        """Test creating a variant."""
        variant = Variant(
            variant_id="var_001",
            name="control",
            description="Control group",
            config={"color": "blue"},
            traffic_percentage=50.0,
            is_control=True,
        )
        
        assert variant.variant_id == "var_001"
        assert variant.name == "control"
        assert variant.traffic_percentage == 50.0
        assert variant.is_control is True
    
    def test_variant_to_dict(self):
        """Test variant serialization."""
        variant = Variant(
            variant_id="var_001",
            name="control",
            config={"color": "blue"},
            traffic_percentage=50.0,
            is_control=True,
        )
        
        data = variant.to_dict()
        assert data["variant_id"] == "var_001"
        assert data["name"] == "control"
        assert data["is_control"] is True
        assert data["traffic_percentage"] == 50.0


class TestMetricConfig:
    """Test metric configuration."""
    
    def test_metric_config_creation(self):
        """Test creating metric config."""
        metric = MetricConfig(
            metric_name="conversion_rate",
            metric_type=MetricType.CONVERSION,
            event_name="purchase",
            is_primary=True,
            minimum_detectable_effect=0.05,
            direction="increase",
        )
        
        assert metric.metric_name == "conversion_rate"
        assert metric.metric_type == MetricType.CONVERSION
        assert metric.is_primary is True
        assert metric.direction == "increase"
    
    def test_metric_config_to_dict(self):
        """Test metric config serialization."""
        metric = MetricConfig(
            metric_name="conversion_rate",
            metric_type=MetricType.CONVERSION,
            event_name="purchase",
            is_primary=True,
        )
        
        data = metric.to_dict()
        assert data["metric_name"] == "conversion_rate"
        assert data["metric_type"] == "conversion"
        assert data["is_primary"] is True


class TestExperimentConfig:
    """Test experiment configuration."""
    
    def test_config_creation(self, sample_variants, sample_metrics):
        """Test creating experiment config."""
        config = ExperimentConfig(
            variants=sample_variants,
            metrics=sample_metrics,
            traffic_split=[50.0, 50.0],
            assignment_method=AssignmentMethod.HASH,
            min_sample_size=1000,
            confidence_level=0.95,
            runtime_days=14,
        )
        
        assert len(config.variants) == 2
        assert len(config.metrics) == 1
        assert config.assignment_method == AssignmentMethod.HASH
        assert config.min_sample_size == 1000
        assert config.confidence_level == 0.95


class TestExperiment:
    """Test experiment model."""
    
    def test_experiment_creation(self, sample_config):
        """Test creating an experiment."""
        experiment = Experiment(
            experiment_id="exp_001",
            name="Test Experiment",
            description="A test experiment",
            experiment_type=ExperimentType.AB_TEST,
            config=sample_config,
            status=ExperimentStatus.DRAFT,
            created_at=datetime.utcnow(),
            created_by="user_001",
        )
        
        assert experiment.experiment_id == "exp_001"
        assert experiment.name == "Test Experiment"
        assert experiment.status == ExperimentStatus.DRAFT
        assert experiment.experiment_type == ExperimentType.AB_TEST


class TestABExperimentFramework:
    """Test experiment framework functionality."""
    
    @pytest.mark.asyncio
    async def test_framework_initialization(self, async_engine):
        """Test framework initialization."""
        fw = ABExperimentFramework(async_engine)
        await fw.initialize()
        assert fw._experiments == {}
    
    @pytest.mark.asyncio
    async def test_create_experiment(self, framework, sample_config):
        """Test creating an experiment."""
        experiment = await framework.create_experiment(
            name="New Recommendation Algorithm",
            description="Testing new algorithm",
            config=sample_config,
            created_by="user_001",
        )
        
        assert experiment.experiment_id.startswith("exp_")
        assert experiment.name == "New Recommendation Algorithm"
        assert experiment.status == ExperimentStatus.DRAFT
        assert experiment.created_by == "user_001"
    
    @pytest.mark.asyncio
    async def test_get_experiment(self, framework, sample_config):
        """Test retrieving an experiment."""
        created = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        retrieved = await framework.get_experiment(created.experiment_id)
        
        assert retrieved is not None
        assert retrieved.experiment_id == created.experiment_id
        assert retrieved.name == "Test Experiment"
    
    @pytest.mark.asyncio
    async def test_get_nonexistent_experiment(self, framework):
        """Test retrieving non-existent experiment."""
        experiment = await framework.get_experiment("nonexistent")
        assert experiment is None
    
    @pytest.mark.asyncio
    async def test_list_experiments(self, framework, sample_config):
        """Test listing experiments."""
        # Create multiple experiments
        for i in range(3):
            await framework.create_experiment(
                name=f"Experiment {i}",
                config=sample_config,
            )
        
        experiments = await framework.list_experiments()
        assert len(experiments) >= 3
    
    @pytest.mark.asyncio
    async def test_start_experiment(self, framework, sample_config):
        """Test starting an experiment."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        success = await framework.start_experiment(experiment.experiment_id)
        assert success is True
        
        # Verify status
        updated = await framework.get_experiment(experiment.experiment_id)
        assert updated.status == ExperimentStatus.RUNNING
        assert updated.started_at is not None
    
    @pytest.mark.asyncio
    async def test_pause_experiment(self, framework, sample_config):
        """Test pausing an experiment."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        success = await framework.pause_experiment(experiment.experiment_id)
        assert success is True
        
        updated = await framework.get_experiment(experiment.experiment_id)
        assert updated.status == ExperimentStatus.PAUSED
    
    @pytest.mark.asyncio
    async def test_stop_experiment(self, framework, sample_config):
        """Test stopping an experiment."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        success = await framework.stop_experiment(experiment.experiment_id)
        assert success is True
        
        updated = await framework.get_experiment(experiment.experiment_id)
        assert updated.status == ExperimentStatus.COMPLETED
        assert updated.ended_at is not None
    
    @pytest.mark.asyncio
    async def test_stop_experiment_with_winner(self, framework, sample_config):
        """Test stopping experiment and declaring winner."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        winner_id = experiment.config.variants[1].variant_id
        
        await framework.stop_experiment(experiment.experiment_id, winner_id)
        
        updated = await framework.get_experiment(experiment.experiment_id)
        assert updated.winner_variant_id == winner_id
    
    @pytest.mark.asyncio
    async def test_assign_user(self, framework, sample_config):
        """Test assigning user to variant."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        assignment = await framework.assign_user(
            experiment_id=experiment.experiment_id,
            user_id="user_123",
        )
        
        assert assignment is not None
        assert assignment.experiment_id == experiment.experiment_id
        assert assignment.user_id == "user_123"
        assert assignment.variant_id in [v.variant_id for v in experiment.config.variants]
    
    @pytest.mark.asyncio
    async def test_assign_user_not_running(self, framework, sample_config):
        """Test assigning user when experiment not running."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        # Don't start the experiment
        assignment = await framework.assign_user(
            experiment_id=experiment.experiment_id,
            user_id="user_123",
        )
        
        assert assignment is None
    
    @pytest.mark.asyncio
    async def test_assign_user_consistent(self, framework, sample_config):
        """Test that user assignment is consistent."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        # Assign same user multiple times
        assignment1 = await framework.assign_user(
            experiment_id=experiment.experiment_id,
            user_id="user_123",
        )
        
        assignment2 = await framework.assign_user(
            experiment_id=experiment.experiment_id,
            user_id="user_123",
        )
        
        # Should get same variant
        assert assignment1.variant_id == assignment2.variant_id
    
    @pytest.mark.asyncio
    async def test_track_event(self, framework, sample_config):
        """Test tracking an event."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        # Assign user first
        assignment = await framework.assign_user(
            experiment_id=experiment.experiment_id,
            user_id="user_123",
        )
        
        # Track event
        event = await framework.track_event(
            experiment_id=experiment.experiment_id,
            user_id="user_123",
            event_name="click",
            event_value=1.0,
        )
        
        assert event is not None
        assert event.experiment_id == experiment.experiment_id
        assert event.user_id == "user_123"
        assert event.variant_id == assignment.variant_id
        assert event.event_name == "click"
    
    @pytest.mark.asyncio
    async def test_track_event_user_not_assigned(self, framework, sample_config):
        """Test tracking event for non-assigned user."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        # Don't assign user, just track
        event = await framework.track_event(
            experiment_id=experiment.experiment_id,
            user_id="user_not_assigned",
            event_name="click",
        )
        
        assert event is None
    
    @pytest.mark.asyncio
    async def test_calculate_results(self, framework, sample_config):
        """Test calculating experiment results."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        # Create assignments and events
        for i in range(100):
            await framework.assign_user(
                experiment_id=experiment.experiment_id,
                user_id=f"user_{i}",
            )
        
        # Add some events
        for i in range(50):
            await framework.track_event(
                experiment_id=experiment.experiment_id,
                user_id=f"user_{i}",
                event_name="click",
                event_value=1.0,
            )
        
        # Calculate results
        results = await framework.calculate_results(experiment.experiment_id)
        
        assert results is not None
        assert results.experiment_id == experiment.experiment_id
        assert results.control_variant is not None
        assert len(results.treatment_variants) >= 1
        assert results.p_value is not None
    
    @pytest.mark.asyncio
    async def test_get_experiment_stats(self, framework, sample_config):
        """Test getting experiment statistics."""
        experiment = await framework.create_experiment(
            name="Test Experiment",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        # Create some assignments
        for i in range(10):
            await framework.assign_user(
                experiment_id=experiment.experiment_id,
                user_id=f"user_{i}",
            )
        
        stats = await framework.get_experiment_stats(experiment.experiment_id)
        
        assert stats is not None
        assert stats["experiment_id"] == experiment.experiment_id
        assert stats["total_assignments"] == 10
        assert "assignments_by_variant" in stats
    
    @pytest.mark.asyncio
    async def test_get_running_experiments(self, framework, sample_config):
        """Test getting running experiments."""
        # Create and start experiment
        experiment = await framework.create_experiment(
            name="Running Experiment",
            config=sample_config,
        )
        await framework.start_experiment(experiment.experiment_id)
        
        # Create draft experiment
        await framework.create_experiment(
            name="Draft Experiment",
            config=sample_config,
        )
        
        running = await framework.get_running_experiments()
        
        assert len(running) >= 1
        assert all(e.status == ExperimentStatus.RUNNING for e in running)
    
    @pytest.mark.asyncio
    async def test_check_experiment_health(self, framework):
        """Test health check."""
        health = await framework.check_experiment_health()
        
        assert health["status"] == "healthy"
        assert "experiments_by_status" in health
        assert "timestamp" in health


class TestAssignmentMethods:
    """Test different assignment methods."""
    
    @pytest.mark.asyncio
    async def test_hash_assignment(self, framework):
        """Test consistent hash assignment."""
        variants = [
            Variant(variant_id="v1", name="control", traffic_percentage=50, is_control=True),
            Variant(variant_id="v2", name="treatment", traffic_percentage=50),
        ]
        
        config = ExperimentConfig(
            variants=variants,
            metrics=[MetricConfig("ctr", MetricType.CONVERSION, "click")],
            assignment_method=AssignmentMethod.HASH,
        )
        
        experiment = await framework.create_experiment(
            name="Hash Test",
            config=config,
        )
        await framework.start_experiment(experiment.experiment_id)
        
        # Same user should always get same variant
        assignments = []
        for _ in range(5):
            a = await framework.assign_user(experiment.experiment_id, "user_123")
            assignments.append(a.variant_id)
        
        assert len(set(assignments)) == 1  # All same
    
    @pytest.mark.asyncio
    async def test_random_assignment(self, framework):
        """Test random assignment."""
        variants = [
            Variant(variant_id="v1", name="control", traffic_percentage=50, is_control=True),
            Variant(variant_id="v2", name="treatment", traffic_percentage=50),
        ]
        
        config = ExperimentConfig(
            variants=variants,
            metrics=[MetricConfig("ctr", MetricType.CONVERSION, "click")],
            assignment_method=AssignmentMethod.RANDOM,
        )
        
        experiment = await framework.create_experiment(
            name="Random Test",
            config=config,
        )
        await framework.start_experiment(experiment.experiment_id)
        
        # Assign many users
        variant_counts = {"v1": 0, "v2": 0}
        for i in range(100):
            a = await framework.assign_user(experiment.experiment_id, f"user_{i}")
            variant_counts[a.variant_id] += 1
        
        # Should be roughly 50/50
        assert 30 < variant_counts["v1"] < 70
        assert 30 < variant_counts["v2"] < 70


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    @pytest.mark.asyncio
    async def test_experiment_without_variants(self, framework):
        """Test experiment with no variants."""
        config = ExperimentConfig(
            variants=[],
            metrics=[MetricConfig("ctr", MetricType.CONVERSION, "click")],
        )
        
        experiment = await framework.create_experiment(
            name="No Variants",
            config=config,
        )
        await framework.start_experiment(experiment.experiment_id)
        
        # Should not be able to assign
        assignment = await framework.assign_user(experiment.experiment_id, "user_123")
        assert assignment is None
    
    @pytest.mark.asyncio
    async def test_start_already_running_experiment(self, framework, sample_config):
        """Test starting an already running experiment."""
        experiment = await framework.create_experiment(
            name="Test",
            config=sample_config,
        )
        
        await framework.start_experiment(experiment.experiment_id)
        
        # Try to start again
        success = await framework.start_experiment(experiment.experiment_id)
        assert success is False  # Should fail
    
    @pytest.mark.asyncio
    async def test_stop_non_running_experiment(self, framework, sample_config):
        """Test stopping a non-running experiment."""
        experiment = await framework.create_experiment(
            name="Test",
            config=sample_config,
        )
        
        # Don't start, just try to stop
        success = await framework.stop_experiment(experiment.experiment_id)
        assert success is False


class TestIntegration:
    """Integration tests for complete workflows."""
    
    @pytest.mark.asyncio
    async def test_full_experiment_workflow(self, async_engine):
        """Test complete experiment lifecycle."""
        framework = ABExperimentFramework(async_engine)
        
        # Create tables
        async with async_engine.begin() as conn:
            for table_sql in [
                """CREATE TABLE IF NOT EXISTS ab_experiments (
                    id INTEGER PRIMARY KEY,
                    experiment_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    experiment_type TEXT DEFAULT 'ab_test',
                    config TEXT NOT NULL,
                    status TEXT DEFAULT 'draft',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    started_at TIMESTAMP,
                    ended_at TIMESTAMP,
                    created_by TEXT,
                    winner_variant_id TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                """CREATE TABLE IF NOT EXISTS ab_user_assignments (
                    id INTEGER PRIMARY KEY,
                    assignment_id TEXT UNIQUE NOT NULL,
                    experiment_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    variant_id TEXT NOT NULL,
                    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    attributes TEXT DEFAULT '{}',
                    UNIQUE(experiment_id, user_id)
                )""",
                """CREATE TABLE IF NOT EXISTS ab_experiment_events (
                    id INTEGER PRIMARY KEY,
                    event_id TEXT UNIQUE NOT NULL,
                    experiment_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    variant_id TEXT NOT NULL,
                    event_name TEXT NOT NULL,
                    event_value REAL DEFAULT 0,
                    event_metadata TEXT DEFAULT '{}',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
                """CREATE TABLE IF NOT EXISTS ab_experiment_results (
                    id INTEGER PRIMARY KEY,
                    result_id TEXT UNIQUE NOT NULL,
                    experiment_id TEXT NOT NULL,
                    metric_name TEXT NOT NULL,
                    results TEXT NOT NULL,
                    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )""",
            ]:
                await conn.execute(text(table_sql))
        
        await framework.initialize()
        
        # 1. Create experiment
        variants = [
            Variant(variant_id="", name="control", traffic_percentage=50, is_control=True),
            Variant(variant_id="", name="treatment", traffic_percentage=50),
        ]
        metrics = [MetricConfig("ctr", MetricType.CONVERSION, "click", is_primary=True)]
        config = ExperimentConfig(variants=variants, metrics=metrics)
        
        experiment = await framework.create_experiment(
            name="Integration Test",
            config=config,
        )
        
        # 2. Start experiment
        await framework.start_experiment(experiment.experiment_id)
        
        # 3. Assign users
        for i in range(50):
            await framework.assign_user(experiment.experiment_id, f"user_{i}")
        
        # 4. Track events
        for i in range(25):
            await framework.track_event(
                experiment.experiment_id,
                f"user_{i}",
                "click",
                1.0,
            )
        
        # 5. Get stats
        stats = await framework.get_experiment_stats(experiment.experiment_id)
        assert stats["total_assignments"] == 50
        
        # 6. Calculate results
        results = await framework.calculate_results(experiment.experiment_id)
        assert results is not None
        
        # 7. Stop experiment
        await framework.stop_experiment(experiment.experiment_id)
        
        # Verify final state
        final = await framework.get_experiment(experiment.experiment_id)
        assert final.status == ExperimentStatus.COMPLETED


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
