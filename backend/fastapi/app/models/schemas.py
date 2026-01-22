from typing import Any, Dict, List, Optional
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field, ConfigDict


class HealthResponse(BaseModel):
    status: str


# ============================================================================
# Authentication Schemas
# ============================================================================

class UserCreate(BaseModel):
    """Schema for creating a new user."""
    username: str = Field(..., min_length=3, max_length=50, description="Unique username")
    password: str = Field(..., min_length=8, description="Password (min 8 characters)")


class UserLogin(BaseModel):
    """Schema for user login."""
    username: str
    password: str


class Token(BaseModel):
    """Schema for JWT token response."""
    access_token: str
    token_type: str


class TokenData(BaseModel):
    """Schema for decoded token data."""
    username: Optional[str] = None


class UserResponse(BaseModel):
    """Schema for user response (excludes password)."""
    id: int
    username: str
    created_at: str
    last_login: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# User CRUD Schemas
# ============================================================================

class UserUpdate(BaseModel):
    """Schema for updating user information."""
    username: Optional[str] = Field(None, min_length=3, max_length=50)
    password: Optional[str] = Field(None, min_length=8)


class UserDetail(BaseModel):
    """Detailed user information including relationships."""
    id: int
    username: str
    created_at: str
    last_login: Optional[str] = None
    has_settings: bool = False
    has_medical_profile: bool = False
    has_personal_profile: bool = False
    has_strengths: bool = False
    has_emotional_patterns: bool = False
    total_assessments: int = 0

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Profile Schemas - User Settings
# ============================================================================

class UserSettingsCreate(BaseModel):
    """Schema for creating user settings."""
    theme: str = Field(default='light', pattern='^(light|dark)$')
    question_count: int = Field(default=10, ge=5, le=50)
    sound_enabled: bool = True
    notifications_enabled: bool = True
    language: str = Field(default='en', min_length=2, max_length=5)


class UserSettingsUpdate(BaseModel):
    """Schema for updating user settings."""
    theme: Optional[str] = Field(None, pattern='^(light|dark)$')
    question_count: Optional[int] = Field(None, ge=5, le=50)
    sound_enabled: Optional[bool] = None
    notifications_enabled: Optional[bool] = None
    language: Optional[str] = Field(None, min_length=2, max_length=5)


class UserSettingsResponse(BaseModel):
    """Schema for user settings response."""
    id: int
    user_id: int
    theme: str
    question_count: int
    sound_enabled: bool
    notifications_enabled: bool
    language: str
    updated_at: str

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Profile Schemas - Medical Profile
# ============================================================================

class MedicalProfileCreate(BaseModel):
    """Schema for creating medical profile."""
    blood_type: Optional[str] = None
    allergies: Optional[str] = None
    medications: Optional[str] = None
    medical_conditions: Optional[str] = None
    surgeries: Optional[str] = None
    therapy_history: Optional[str] = None
    ongoing_health_issues: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_phone: Optional[str] = None


class MedicalProfileUpdate(BaseModel):
    """Schema for updating medical profile."""
    blood_type: Optional[str] = None
    allergies: Optional[str] = None
    medications: Optional[str] = None
    medical_conditions: Optional[str] = None
    surgeries: Optional[str] = None
    therapy_history: Optional[str] = None
    ongoing_health_issues: Optional[str] = None
    emergency_contact_name: Optional[str] = None
    emergency_contact_phone: Optional[str] = None


class MedicalProfileResponse(BaseModel):
    """Schema for medical profile response."""
    id: int
    user_id: int
    blood_type: Optional[str]
    allergies: Optional[str]
    medications: Optional[str]
    medical_conditions: Optional[str]
    surgeries: Optional[str]
    therapy_history: Optional[str]
    ongoing_health_issues: Optional[str]
    emergency_contact_name: Optional[str]
    emergency_contact_phone: Optional[str]
    last_updated: str

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Profile Schemas - Personal Profile
# ============================================================================

class PersonalProfileCreate(BaseModel):
    """Schema for creating personal profile."""
    occupation: Optional[str] = None
    education: Optional[str] = None
    marital_status: Optional[str] = None
    hobbies: Optional[str] = None
    bio: Optional[str] = Field(None, max_length=1000)
    life_events: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    date_of_birth: Optional[str] = None
    gender: Optional[str] = None
    address: Optional[str] = None
    society_contribution: Optional[str] = None
    life_pov: Optional[str] = None
    high_pressure_events: Optional[str] = None
    avatar_path: Optional[str] = None


class PersonalProfileUpdate(BaseModel):
    """Schema for updating personal profile."""
    occupation: Optional[str] = None
    education: Optional[str] = None
    marital_status: Optional[str] = None
    hobbies: Optional[str] = None
    bio: Optional[str] = Field(None, max_length=1000)
    life_events: Optional[str] = None
    email: Optional[EmailStr] = None
    phone: Optional[str] = None
    date_of_birth: Optional[str] = None
    gender: Optional[str] = None
    address: Optional[str] = None
    society_contribution: Optional[str] = None
    life_pov: Optional[str] = None
    high_pressure_events: Optional[str] = None
    avatar_path: Optional[str] = None


class PersonalProfileResponse(BaseModel):
    """Schema for personal profile response."""
    id: int
    user_id: int
    occupation: Optional[str]
    education: Optional[str]
    marital_status: Optional[str]
    hobbies: Optional[str]
    bio: Optional[str]
    life_events: Optional[str]
    email: Optional[str]
    phone: Optional[str]
    date_of_birth: Optional[str]
    gender: Optional[str]
    address: Optional[str]
    society_contribution: Optional[str]
    life_pov: Optional[str]
    high_pressure_events: Optional[str]
    avatar_path: Optional[str]
    last_updated: str

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Profile Schemas - User Strengths
# ============================================================================

class UserStrengthsCreate(BaseModel):
    """Schema for creating user strengths."""
    top_strengths: str = "[]"
    areas_for_improvement: str = "[]"
    current_challenges: str = "[]"
    learning_style: Optional[str] = None
    communication_preference: Optional[str] = None
    comm_style: Optional[str] = None
    sharing_boundaries: str = "[]"
    goals: Optional[str] = None


class UserStrengthsUpdate(BaseModel):
    """Schema for updating user strengths."""
    top_strengths: Optional[str] = None
    areas_for_improvement: Optional[str] = None
    current_challenges: Optional[str] = None
    learning_style: Optional[str] = None
    communication_preference: Optional[str] = None
    comm_style: Optional[str] = None
    sharing_boundaries: Optional[str] = None
    goals: Optional[str] = None


class UserStrengthsResponse(BaseModel):
    """Schema for user strengths response."""
    id: int
    user_id: int
    top_strengths: str
    areas_for_improvement: str
    current_challenges: str
    learning_style: Optional[str]
    communication_preference: Optional[str]
    comm_style: Optional[str]
    sharing_boundaries: str
    goals: Optional[str]
    last_updated: str

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Profile Schemas - Emotional Patterns
# ============================================================================

class UserEmotionalPatternsCreate(BaseModel):
    """Schema for creating emotional patterns."""
    common_emotions: str = "[]"
    emotional_triggers: Optional[str] = None
    coping_strategies: Optional[str] = None
    preferred_support: Optional[str] = None


class UserEmotionalPatternsUpdate(BaseModel):
    """Schema for updating emotional patterns."""
    common_emotions: Optional[str] = None
    emotional_triggers: Optional[str] = None
    coping_strategies: Optional[str] = None
    preferred_support: Optional[str] = None


class UserEmotionalPatternsResponse(BaseModel):
    """Schema for emotional patterns response."""
    id: int
    user_id: int
    common_emotions: str
    emotional_triggers: Optional[str]
    coping_strategies: Optional[str]
    preferred_support: Optional[str]
    last_updated: str

    model_config = ConfigDict(from_attributes=True)


# ============================================================================
# Comprehensive Profile Response
# ============================================================================

class CompleteProfileResponse(BaseModel):
    """Complete user profile with all sub-profiles."""
    user: UserResponse
    settings: Optional[UserSettingsResponse] = None
    medical_profile: Optional[MedicalProfileResponse] = None
    personal_profile: Optional[PersonalProfileResponse] = None
    strengths: Optional[UserStrengthsResponse] = None
    emotional_patterns: Optional[UserEmotionalPatternsResponse] = None


class AssessmentSummary(BaseModel):
    assessment_type: str
    total_responses: int
    highest_score: Optional[int]
    average_score: Optional[float]
    latest_timestamp: Optional[str]


class AssessmentEntry(BaseModel):
    id: int
    total_score: int
    details: Dict[str, Any]
    timestamp: str


class AssessmentDetail(BaseModel):
    assessment_type: str
    entries: List[AssessmentEntry]


class UserResponse(BaseModel):
    id: int
    username: str
    created_at: str


# Analytics schemas
class AgeGroupStats(BaseModel):
    """Aggregated statistics by age group"""
    age_group: str
    total_assessments: int
    average_score: float
    min_score: int
    max_score: int
    average_sentiment: float


class ScoreDistribution(BaseModel):
    """Score distribution for analytics"""
    score_range: str
    count: int
    percentage: float


class TrendDataPoint(BaseModel):
    """Time-series data point"""
    period: str
    average_score: float
    assessment_count: int


class AnalyticsSummary(BaseModel):
    """Overall analytics summary - aggregated data only"""
    total_assessments: int = Field(description="Total number of assessments")
    unique_users: int = Field(description="Number of unique users")
    global_average_score: float = Field(description="Overall average score")
    global_average_sentiment: float = Field(description="Overall sentiment score")
    age_group_stats: List[AgeGroupStats] = Field(description="Stats by age group")
    score_distribution: List[ScoreDistribution] = Field(description="Score distribution")
    assessment_quality_metrics: Dict[str, int] = Field(
        description="Quality metrics (rushed, inconsistent counts)"
    )


class TrendAnalytics(BaseModel):
    """Trend analytics over time"""
    period_type: str = Field(description="Time period type (daily, weekly, monthly)")
    data_points: List[TrendDataPoint] = Field(description="Time series data")
    trend_direction: str = Field(description="Overall trend (increasing, decreasing, stable)")


class BenchmarkComparison(BaseModel):
    """Benchmark comparison data"""
    category: str
    global_average: float
    percentile_25: float
    percentile_50: float
    percentile_75: float
    percentile_90: float


class PopulationInsights(BaseModel):
    """Population-level insights - no individual data"""
    most_common_age_group: str
    highest_performing_age_group: str
    total_population_size: int
    assessment_completion_rate: Optional[float] = Field(
        default=None, 
        description="Percentage of started assessments that were completed"
    )
