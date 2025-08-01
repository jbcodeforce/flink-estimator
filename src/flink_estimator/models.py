"""
Pydantic models for Flink Resource Estimator.

This module contains all data models used for input validation,
estimation results, and file persistence.
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional


class EstimationInput(BaseModel):
    """Input parameters for Flink estimation"""
    project_name: str = Field(..., min_length=1, max_length=100, description="Name of the project")
    messages_per_second: int = Field(..., gt=0, description="Expected messages per second")
    avg_record_size_bytes: int = Field(..., gt=0, description="Average record size in bytes")
    simple_statements: int = Field(default=0, ge=0, description="Number of simple statements")
    medium_statements: int = Field(default=0, ge=0, description="Number of medium complexity statements")
    complex_statements: int = Field(default=0, ge=0, description="Number of complex statements")
    
    @field_validator('project_name')
    def validate_project_name(cls, v):
        if not v or v.isspace():
            raise ValueError('Project name cannot be empty or just whitespace')
        return v.strip()
    
    @property
    def total_statements(self) -> int:
        return self.simple_statements + self.medium_statements + self.complex_statements
    
    @property
    def total_throughput_mb_per_sec(self) -> float:
        return (self.messages_per_second * self.avg_record_size_bytes) / (1024 * 1024)


class InputSummary(BaseModel):
    """Summary of input parameters with calculated values"""
    messages_per_second: int
    avg_record_size_bytes: int
    total_throughput_mb_per_sec: float
    simple_statements: int
    medium_statements: int
    complex_statements: int
    total_statements: int


class ResourceEstimates(BaseModel):
    """Estimated resource requirements"""
    total_memory_mb: int
    total_cpu_cores: int
    processing_load_score: float


class JobManagerConfig(BaseModel):
    """JobManager configuration specifications"""
    memory_mb: int
    cpu_cores: int


class TaskManagerConfig(BaseModel):
    """TaskManager configuration specifications"""
    count: int
    memory_mb_each: int
    cpu_cores_each: int
    total_memory_mb: int
    total_cpu_cores: int


class ClusterRecommendations(BaseModel):
    """Cluster configuration recommendations"""
    jobmanager: JobManagerConfig
    taskmanagers: TaskManagerConfig


class ScalingRecommendations(BaseModel):
    """Scaling and performance recommendations"""
    min_parallelism: int
    recommended_parallelism: int
    max_parallelism: int
    checkpointing_interval_ms: int


class EstimationResult(BaseModel):
    """Complete estimation result"""
    input_summary: InputSummary
    resource_estimates: ResourceEstimates
    cluster_recommendations: ClusterRecommendations
    scaling_recommendations: ScalingRecommendations


class EstimationMetadata(BaseModel):
    """Metadata for saved estimations"""
    estimation_id: str
    timestamp: str
    project_name: str
    saved_at: str


class SavedEstimation(BaseModel):
    """Complete saved estimation data structure"""
    metadata: EstimationMetadata
    input_parameters: EstimationInput
    estimation_results: EstimationResult
    version: str = "1.0"