"""
Flink resource estimation calculation logic.

This module contains the core business logic for calculating
Flink resource requirements and file persistence operations.
"""

import math
import os
import uuid
from datetime import datetime
from typing import Optional

from .models import (
    EstimationInput,
    EstimationResult,
    InputSummary,
    ResourceEstimates,
    JobManagerConfig,
    TaskManagerConfig,
    ClusterRecommendations,
    ScalingRecommendations,
    EstimationMetadata,
    SavedEstimation
)

# Configuration
SAVED_ESTIMATIONS_DIR = "saved_estimations"
os.makedirs(SAVED_ESTIMATIONS_DIR, exist_ok=True)


def calculate_flink_estimation(input_params: EstimationInput) -> EstimationResult:
    """
    Calculate Flink resource estimation based on input parameters.
    This is a simplified estimation model that can be easily unit tested.
    
    Args:
        input_params: Validated input parameters for the estimation
        
    Returns:
        EstimationResult: Complete estimation with resource recommendations
    """
    
    # Use properties from EstimationInput for calculations
    total_throughput_mb = input_params.total_throughput_mb_per_sec
    
    # Complexity multipliers (processing overhead)
    simple_multiplier = 1.2
    medium_multiplier = 2.0
    complex_multiplier = 3.5
    
    # Calculate processing load
    processing_load = (
        input_params.simple_statements * simple_multiplier +
        input_params.medium_statements * medium_multiplier +
        input_params.complex_statements * complex_multiplier
    )
    
    # Memory estimation (MB)
    # Base memory per statement + buffer for throughput
    base_memory_per_statement = 512  # MB
    throughput_memory_factor = total_throughput_mb * 2  # Buffer factor
    
    total_memory_mb = (
        input_params.total_statements * base_memory_per_statement +
        throughput_memory_factor +
        processing_load * 100  # Additional memory for complex processing
    )
    
    # CPU estimation (cores)
    # Base CPU + processing complexity + throughput factor
    base_cpu_cores = 2
    throughput_cpu_factor = math.ceil(total_throughput_mb / 50)  # 1 core per 50MB/s
    complexity_cpu = math.ceil(processing_load / 2)
    
    total_cpu_cores = base_cpu_cores + throughput_cpu_factor + complexity_cpu
    
    # TaskManager recommendations
    taskmanager_memory_mb = min(8192, max(2048, total_memory_mb // 2))  # 2-8GB per TM
    taskmanager_cpu_cores = min(8, max(2, total_cpu_cores // 2))  # 2-8 cores per TM
    
    # Number of TaskManagers needed
    num_taskmanagers = max(2, math.ceil(total_memory_mb / taskmanager_memory_mb))
    
    # JobManager specs (usually fixed)
    jobmanager_memory_mb = max(1024, min(4096, total_memory_mb // 8))
    jobmanager_cpu_cores = 2
    
    # Create Pydantic models for the result
    input_summary = InputSummary(
        messages_per_second=input_params.messages_per_second,
        avg_record_size_bytes=input_params.avg_record_size_bytes,
        total_throughput_mb_per_sec=round(total_throughput_mb, 2),
        simple_statements=input_params.simple_statements,
        medium_statements=input_params.medium_statements,
        complex_statements=input_params.complex_statements,
        total_statements=input_params.total_statements
    )
    
    resource_estimates = ResourceEstimates(
        total_memory_mb=math.ceil(total_memory_mb),
        total_cpu_cores=total_cpu_cores,
        processing_load_score=round(processing_load, 2)
    )
    
    jobmanager_config = JobManagerConfig(
        memory_mb=jobmanager_memory_mb,
        cpu_cores=jobmanager_cpu_cores
    )
    
    taskmanager_config = TaskManagerConfig(
        count=num_taskmanagers,
        memory_mb_each=taskmanager_memory_mb,
        cpu_cores_each=taskmanager_cpu_cores,
        total_memory_mb=num_taskmanagers * taskmanager_memory_mb,
        total_cpu_cores=num_taskmanagers * taskmanager_cpu_cores
    )
    
    cluster_recommendations = ClusterRecommendations(
        jobmanager=jobmanager_config,
        taskmanagers=taskmanager_config
    )
    
    scaling_recommendations = ScalingRecommendations(
        min_parallelism=max(1, total_cpu_cores // 2),
        recommended_parallelism=total_cpu_cores,
        max_parallelism=total_cpu_cores * 2,
        checkpointing_interval_ms=min(60000, max(5000, 10000 + int(processing_load * 1000)))
    )
    
    return EstimationResult(
        input_summary=input_summary,
        resource_estimates=resource_estimates,
        cluster_recommendations=cluster_recommendations,
        scaling_recommendations=scaling_recommendations
    )


def save_estimation_to_json(
    input_params: EstimationInput,
    estimation_result: EstimationResult
) -> str:
    """
    Save estimation data to a JSON file with timestamp and unique ID.
    
    Args:
        input_params: The input parameters used for estimation
        estimation_result: The calculated estimation results
        
    Returns:
        str: The filename of the saved file
        
    Raises:
        OSError: If there's an error creating directories or writing the file
    """
    
    # Generate unique identifier and timestamp
    estimation_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now().isoformat()
    
    # Create metadata
    metadata = EstimationMetadata(
        estimation_id=estimation_id,
        timestamp=timestamp,
        project_name=input_params.project_name,
        saved_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    
    # Create complete saved estimation object
    saved_estimation = SavedEstimation(
        metadata=metadata,
        input_parameters=input_params,
        estimation_results=estimation_result
    )
    
    # Create filename with project name and timestamp
    safe_project_name = "".join(c for c in input_params.project_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    safe_project_name = safe_project_name.replace(' ', '_')
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_project_name}_{timestamp_str}_{estimation_id}.json"
    filepath = os.path.join(SAVED_ESTIMATIONS_DIR, filename)
    
    # Save to file using Pydantic's JSON serialization
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(saved_estimation.model_dump_json(indent=2))
    
    return filename


def get_saved_estimations_directory() -> str:
    """
    Get the directory path where estimations are saved.
    
    Returns:
        str: Path to the saved estimations directory
    """
    return SAVED_ESTIMATIONS_DIR