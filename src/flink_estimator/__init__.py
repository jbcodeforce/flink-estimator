"""
Flink Resource Estimator Package

A FastAPI web application for estimating Apache Flink cluster resource requirements.
Provides both web UI and REST API interfaces with Pydantic models for data validation.
"""

__version__ = "1.0.0"
__author__ = "Flink Estimator Team"

# Import main components for easy access
from .models import (
    EstimationInput,
    EstimationResult,
    SavedEstimation,
    EstimationMetadata
)

from .estimation import (
    calculate_flink_estimation,
    save_estimation_to_json,
    get_saved_estimations_directory
)

# Make key components available at package level
__all__ = [
    "EstimationInput",
    "EstimationResult", 
    "SavedEstimation",
    "EstimationMetadata",
    "calculate_flink_estimation",
    "save_estimation_to_json",
    "get_saved_estimations_directory"
]