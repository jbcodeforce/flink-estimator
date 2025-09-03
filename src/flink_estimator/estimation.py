"""
Flink resource estimation calculation logic.

This module contains the core business logic for calculating
Flink resource requirements and file persistence operations.

So rule of thumb:
Flink can process 10000 records per second per core.
But those numbers may go lower with bigger messages, bigger state, key skew, number of disctint keys, etc.

Statmeent complexity reflect the usage of complex operators, like joins, windowed aggregations, etc.

Source and Sink latency are not considered and assumed to be minimal.
Assumed the key size is minimal, a few bytes

Bandwidth capacity is the network capacity of the cluster.

A Flink job is composed of a graph of operators, Operators are deployed, chained in threads, and executed in task managers with a configured parallelism
* Each Operator has its own costs
* Working State is kept locally in RocksDB impacting storage requirements
* But is backed up and restored remotely to distributed storage impacting checkpoint size and latency
* Depends on operator structure and settings and message size
* Checkpoint copies state for recovery
* Checkpoint Interval determines how frequently state is captured
* Aggregate State Size consumes bandwidth, determines recovery time which impacts latency

The minimum number of nodes for Flink is 3. Each Flink Node may not exceed 8 CPU Cores per Flink Node. 
CPU means an actual bare metal processing unit that has at least one CPU Core. Multi-core or hyperthreading processors are counted as one CPU.
A CPU Core refers to “cpu units” in Kubernetes. 1 CPU is equivalent to one AWS vCPU, 1 GCP core, 1 Azure vCore or 1 hyperthread on a bare-metal processor with hyperthreading enabled.
CP-Flink node-based pricing is purposefully distinct from the number of nodes in the K8s cluster.

Consider a Flink node (running task manager) to be a 4 CPU node, with 16GB of memory. They should process 20 to 50 MB/s of data.
Typical Flink jobs have a lot of state and benefit for more memory.

Think to scale vertically before horizontally.

Heuristics:
* increase the number of task managers until there is enough resource to get the expected throughput and recovery times
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
MEDIUM_SKEW_FACTOR = 1.2
BASE_CPU_CORES = .7
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
    total_throughput_mbps = input_params.total_throughput_mb_per_sec
    
    # Convert bandwidth from Mbps to MB/s for comparison
    bandwidth_capacity_mb_per_sec = input_params.bandwidth_capacity_mbps / 8
    
    # Check if throughput exceeds bandwidth capacity
    bandwidth_utilization = total_throughput_mbps / bandwidth_capacity_mb_per_sec if bandwidth_capacity_mb_per_sec > 0 else 0
    
    # Adjust cpu needs if bandwidth is a bottleneck
    if bandwidth_utilization > 0.8:  # If using more than 80% of bandwidth
        # Add CPU overhead for compression/optimization needed
        bandwidth_cpu_penalty = 2
    else:
        bandwidth_cpu_penalty = 0
    
    # Factor in distinct keys for state partitioning overhead
    key_distribution_cpu_factor = min(2.0, math.log10(input_params.num_distinct_keys) / 2)
    
    # Data skew risk multipliers - affects resource allocation and parallelism
    skew_multipliers = {
        "low": 1.0,     # Even distribution, no additional overhead
        "medium": MEDIUM_SKEW_FACTOR,  # Some skew, moderate additional resources needed
        "high": 1.7     # Significant skew, substantial additional resources needed
    }
    skew_factor = skew_multipliers.get(input_params.data_skew_risk, MEDIUM_SKEW_FACTOR)
    
    # Statement complexity multipliers
    simple_multiplier = .25
    medium_multiplier = 1
    complex_multiplier = 1.5
    
    # Calculate processing load
    processing_load = (
        input_params.simple_statements * simple_multiplier +
        input_params.medium_statements * medium_multiplier +
        input_params.complex_statements * complex_multiplier
    ) * key_distribution_cpu_factor  # Multiply by key distribution factor
    
    # Memory estimation (MB)
    # Base memory per statement + buffer for throughput + state management
    base_memory_per_statement = 1024  # MB
    throughput_memory_factor = total_throughput_mbps * 2  # Buffer factor
    
    # Additional memory for state management based on distinct keys
    state_memory_factor = math.log10(input_params.num_distinct_keys) * 100  # More keys = more state
    
    # Additional memory for low-latency buffering and optimization
    latency_memory_overhead = 0
    if input_params.expected_latency_seconds <= 1.0:
        # Low latency needs more buffering and in-memory optimization
        latency_memory_overhead = total_throughput_mbps * 1000  # MB for buffering
    elif input_params.expected_latency_seconds <= 5.0:
        latency_memory_overhead = total_throughput_mbps * 500   # Moderate buffering
    
    # Apply skew factor to memory - high skew requires more memory for hotspots
    total_memory_mb = (
        input_params.total_statements * base_memory_per_statement +
        throughput_memory_factor +
        processing_load * 100 +  # Additional memory for complex processing
        state_memory_factor +    # Memory for state management
        latency_memory_overhead  # Extra memory for low-latency processing
    ) * skew_factor
    
    # CPU estimation (cores)
    # Base CPU + processing complexity + throughput factor + bandwidth considerations
    base_cpu_cores = BASE_CPU_CORES
    throughput_cpu_factor = math.ceil(total_throughput_mbps / 50)  # 1 core per 50MB/s
    complexity_cpu = math.ceil(processing_load / 2)
    
    # Latency factor - stricter latency requirements need more resources
    # Lower latency (< 1s) needs additional overhead for optimization
    if input_params.expected_latency_seconds <= 0.5:
        latency_factor = 1.5  # 50% more resources for sub-500ms latency
    elif input_params.expected_latency_seconds <= 1.0:
        latency_factor = 1.2  # 20% more resources for sub-1s latency
    elif input_params.expected_latency_seconds <= 5.0:
        latency_factor = 1.1  # 10% more resources for sub-5s latency
    else:
        latency_factor = 1.0  # Normal resources for relaxed latency (>5s)
    
    # Apply skew factor and latency factor to CPU
    total_cpu_cores = (base_cpu_cores + throughput_cpu_factor + complexity_cpu + bandwidth_cpu_penalty) * skew_factor * latency_factor
    total_cpu_cores = math.ceil(total_cpu_cores)  # Round up to whole cores
    
    # TaskManager recommendations
    taskmanager_memory_mb = min(8192, max(2048, total_memory_mb // 2))  # 2-8GB per TM
    taskmanager_cpu_cores = min(8, max(2, total_cpu_cores // 2))  # 2-8 cores per TM
    
    # Number of TaskManagers needed
    num_taskmanagers = max(1, math.ceil(total_memory_mb / taskmanager_memory_mb))
    
    # JobManager specs (usually fixed)
    jobmanager_memory_mb = max(1024, min(4096, total_memory_mb // 8))
    jobmanager_cpu_cores = BASE_CPU_CORES
    
    # Create Pydantic models for the result
    input_summary = InputSummary(
        messages_per_second=input_params.messages_per_second,
        avg_record_size_bytes=input_params.avg_record_size_bytes,
        total_throughput_mb_per_sec=round(total_throughput_mbps, 2),
        num_distinct_keys=input_params.num_distinct_keys,
        data_skew_risk=input_params.data_skew_risk,
        bandwidth_capacity_mbps=input_params.bandwidth_capacity_mbps,
        expected_latency_seconds=input_params.expected_latency_seconds,
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
        memory_mb=math.ceil(jobmanager_memory_mb),
        cpu_cores=math.ceil(jobmanager_cpu_cores)
    )
    
    taskmanager_config = TaskManagerConfig(
        count=num_taskmanagers,
        memory_mb_each=math.ceil(taskmanager_memory_mb),
        cpu_cores_each=taskmanager_cpu_cores,
        total_memory_mb=num_taskmanagers * math.ceil(taskmanager_memory_mb),
        total_cpu_cores=num_taskmanagers * taskmanager_cpu_cores
    )
    
    cluster_recommendations = ClusterRecommendations(
        jobmanager=jobmanager_config,
        taskmanagers=taskmanager_config
    )
    
    # Adjust parallelism recommendations based on skew risk and latency requirements
    if input_params.data_skew_risk == "high":
        # For high skew, recommend more conservative parallelism to avoid hotspots
        min_parallelism = max(1, total_cpu_cores // 3)
        recommended_parallelism = max(total_cpu_cores // 2, min(input_params.num_distinct_keys // 1000, total_cpu_cores))
        max_parallelism = total_cpu_cores
        # Longer checkpointing for skewed data
        base_checkpoint_interval = min(60000, max(10000, 15000 + int(processing_load * 1000)))
    elif input_params.data_skew_risk == "medium":
        min_parallelism = max(1, total_cpu_cores // 2)
        recommended_parallelism = min(total_cpu_cores, max(total_cpu_cores // 2, input_params.num_distinct_keys // 2000))
        max_parallelism = total_cpu_cores * 2
        base_checkpoint_interval = min(60000, max(5000, 10000 + int(processing_load * 1000)))
    else:  # low skew
        min_parallelism = max(1, total_cpu_cores // 2)
        recommended_parallelism = total_cpu_cores
        max_parallelism = total_cpu_cores * 2
        base_checkpoint_interval = min(60000, max(5000, 8000 + int(processing_load * 1000)))
    
    # Adjust checkpoint interval based on latency requirements
    # Stricter latency needs more frequent checkpoints for faster recovery
    if input_params.expected_latency_seconds <= 0.5:
        # Very strict latency: checkpoint every 3-5 seconds
        checkpoint_interval = min(base_checkpoint_interval, 5000)
    elif input_params.expected_latency_seconds <= 1.0:
        # Strict latency: checkpoint every 5-10 seconds  
        checkpoint_interval = min(base_checkpoint_interval, 10000)
    elif input_params.expected_latency_seconds <= 5.0:
        # Moderate latency: checkpoint every 10-20 seconds
        checkpoint_interval = min(base_checkpoint_interval, 20000)
    else:
        # Relaxed latency: use base interval
        checkpoint_interval = base_checkpoint_interval
    
    # For very low latency, consider increasing parallelism for faster processing
    if input_params.expected_latency_seconds <= 1.0:
        # Boost recommended parallelism for low latency
        parallelism_boost = max(1, int(2.0 / input_params.expected_latency_seconds))
        recommended_parallelism = min(max_parallelism, recommended_parallelism * parallelism_boost)
    
    scaling_recommendations = ScalingRecommendations(
        min_parallelism=min_parallelism,
        recommended_parallelism=recommended_parallelism,
        max_parallelism=max_parallelism,
        checkpointing_interval_ms=checkpoint_interval
    )
    
    result= EstimationResult(
        input_summary=input_summary,
        resource_estimates=resource_estimates,
        cluster_recommendations=cluster_recommendations,
        scaling_recommendations=scaling_recommendations
    )
    print("-"*80)
    print(result.model_dump_json(indent=2))
    print("-"*80)
    return result


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