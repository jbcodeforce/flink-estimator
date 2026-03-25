"""
Flink resource estimation calculation logic.

This module contains the core business logic for calculating
Flink resource requirements and file persistence operations.

Public API: ``calculate_flink_estimation``, ``save_estimation_to_json``,
``get_saved_estimations_directory``. Other callables are module-private (``_`` prefix).

So rule of thumb:
Flink can process 10000 records per second per core.
But those numbers may go lower with bigger messages, bigger state, key skew, number of disctint keys, etc.

Statmeent complexity reflect the usage of complex operators, like joins, windowed aggregations, etc.

Source and Sink latency are not considered and assumed to be minimal.
Assumed the key size is minimal, a few bytes

Bandwidth capacity is the network capacity of the cluster in megabits per second (Mbps).
Throughput is compared to capacity after converting MB/s to Mbps (multiply by 8).

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
A CPU Core refers to "cpu units" in Kubernetes. 1 CPU is equivalent to one AWS vCPU, 1 GCP core, 1 Azure vCore or 1 hyperthread on a bare-metal processor with hyperthreading enabled.
CP-Flink node-based pricing is purposefully distinct from the number of nodes in the K8s cluster.

Consider a Flink node (running task manager) to be a 4 CPU node, with 16GB of memory. They should process 20 to 50 MB/s of data.
Typical Flink jobs have a lot of state and benefit for more memory.

Think to scale vertically before horizontally.

Heuristics:
* increase the number of task managers until there is enough resource to get the expected throughput and recovery times

total_nodes is a coarse count from total CPU and an 8-cores-per-node assumption (minimum 3). TaskManager count is sized separately from memory and CPU needs and may differ from total_nodes.
"""

import math
import os
import uuid
from datetime import datetime

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
    SavedEstimation,
)

# Configuration
SAVED_ESTIMATIONS_DIR = "saved_estimations"

# OLD parameters
LOW_SKEW_FACTOR = 1.0
MEDIUM_SKEW_FACTOR = 1.2
HIGH_SKEW_FACTOR = 1.7
BASE_CPU_CORES = 0.7
JOBMANAGER_CPU_CORES = 0.5  # Minimum viable JM CPU (Kubernetes cpu units)
MIN_TASKMANAGER_CPU_CORES = 0.5
SIMPLE_MULTIPLIER = 0.25
MEDIUM_MULTIPLIER = 1.0
COMPLEX_MULTIPLIER = 1.2
BASE_MEMORY_PER_STATEMENT = 256  # MB per statement (operator) baseline
PROCESSING_LOAD_MEMORY_SCALE = 50  # MB per unit of processing_load

# Key cardinality: bounded CPU load multiplier (log-scaled, not a hard cap at 1.0)
KEY_CPU_FACTOR_MIN = 0.5
KEY_CPU_FACTOR_MAX = 2.0
KEY_CPU_LOG_ANCHOR = 3.0  # log10(keys) at which factor is 1.0 (1000 keys)
KEY_CPU_LOG_COEFF = 0.2

# Latency-related memory: throughput * latency * scale, capped vs rest of memory estimate
LATENCY_MEMORY_MB_PER_MBPS_S = 40.0
LATENCY_MEMORY_MAX_FRACTION_OF_SUBTOTAL = 0.45

# CPU: skew is softened on CPU (memory still uses full skew) to avoid double-counting with memory skew
CPU_SKEW_BLEND = 0.5  # cpu uses 1 + (skew-1)*CPU_SKEW_BLEND
# Integer cores after ceil can tie across skew tiers; small additive bump preserves ordering low < medium < high
SKEW_CPU_EXTRA_CORES = {"low": 0, "medium": 1, "high": 2}

BANDWIDTH_UTILIZATION_THRESHOLD = 0.8
BANDWIDTH_CPU_PENALTY_CORES = 2

# MB/s to megabits per second (network line rate)
MBPS_TO_MEGABITS_PER_SEC = 8.0


def _key_distribution_cpu_factor(num_distinct_keys: int) -> float:
    """Bounded multiplier for processing load from key cardinality (state/partition pressure)."""
    x = math.log10(max(1, num_distinct_keys))
    raw = 1.0 + KEY_CPU_LOG_COEFF * (x - KEY_CPU_LOG_ANCHOR)
    return max(KEY_CPU_FACTOR_MIN, min(KEY_CPU_FACTOR_MAX, raw))


def _bandwidth_cpu_penalty(total_throughput_mb_per_sec: float, bandwidth_capacity_mbps: int) -> int:
    """Extra CPU when estimated data rate uses a large share of cluster network capacity."""
    if bandwidth_capacity_mbps <= 0:
        return 0
    throughput_mbps = total_throughput_mb_per_sec * MBPS_TO_MEGABITS_PER_SEC
    utilization = throughput_mbps / bandwidth_capacity_mbps
    return BANDWIDTH_CPU_PENALTY_CORES if utilization > BANDWIDTH_UTILIZATION_THRESHOLD else 0


def _cpu_skew_multiplier(skew_factor: float) -> float:
    """Reduce skew impact on CPU vs memory to limit double-counting."""
    return 1.0 + (skew_factor - 1.0) * CPU_SKEW_BLEND


def _latency_cpu_factor(expected_latency_seconds: float) -> float:
    if expected_latency_seconds <= 0.5:
        return 1.5
    if expected_latency_seconds <= 1.0:
        return 1.2
    if expected_latency_seconds <= 5.0:
        return 1.1
    return 1.0


def _compute_processing_load(input_params: EstimationInput) -> float:
    key_f = _key_distribution_cpu_factor(input_params.num_distinct_keys)
    return (
        input_params.simple_statements * SIMPLE_MULTIPLIER
        + input_params.medium_statements * MEDIUM_MULTIPLIER
        + input_params.complex_statements * COMPLEX_MULTIPLIER
    ) * key_f


def _compute_memory_mb(
    input_params: EstimationInput,
    total_throughput_mb_per_sec: float,
    processing_load: float,
    skew_factor: float,
) -> tuple[float, float]:
    """
    Returns (total_memory_mb, memory_subtotal_before_latency_skew).
    Skew applies to full memory including latency overhead.
    """
    base_memory_per_statement = BASE_MEMORY_PER_STATEMENT
    throughput_memory_factor = total_throughput_mb_per_sec * 2
    state_memory_factor = math.log10(max(1, input_params.num_distinct_keys)) * 100
    subtotal = (
        input_params.total_statements * base_memory_per_statement
        + throughput_memory_factor
        + processing_load * PROCESSING_LOAD_MEMORY_SCALE
        + state_memory_factor
    )
    lat_cap = LATENCY_MEMORY_MAX_FRACTION_OF_SUBTOTAL * max(subtotal, 1.0)
    if input_params.expected_latency_seconds > 5.0:
        latency_memory_overhead = 0.0
    else:
        latency_window = min(input_params.expected_latency_seconds, 5.0)
        raw_latency_mem = total_throughput_mb_per_sec * latency_window * LATENCY_MEMORY_MB_PER_MBPS_S
        latency_memory_overhead = min(raw_latency_mem, lat_cap)
    total = (subtotal + latency_memory_overhead) * skew_factor
    return total, subtotal


def _compute_cpu_cores(
    input_params: EstimationInput,
    total_throughput_mb_per_sec: float,
    processing_load: float,
    bandwidth_cpu_penalty: int,
    skew_factor: float,
) -> int:
    base_cpu = BASE_CPU_CORES
    throughput_cpu = math.ceil(total_throughput_mb_per_sec / 50)
    complexity_cpu = math.ceil(processing_load / 2)
    subtotal = base_cpu + throughput_cpu + complexity_cpu + bandwidth_cpu_penalty
    skew_cpu = _cpu_skew_multiplier(skew_factor)
    latency_f = _latency_cpu_factor(input_params.expected_latency_seconds)
    skew_extra = SKEW_CPU_EXTRA_CORES.get(input_params.data_skew_risk, 1)
    total = subtotal * skew_cpu * latency_f + skew_extra
    return max(1, math.ceil(total))


def _compute_total_nodes(total_cpu_cores: int) -> int:
    return max(3, math.ceil(total_cpu_cores / 8))


def _compute_taskmanager_layout(
    input_params: EstimationInput,
    total_memory_mb: float,
    total_cpu_cores: int,
) -> tuple[int, int, int]:
    """
    Size TaskManagers so total memory and CPU fit.

    Memory-bound: use as few TMs as possible by filling each up to taskmanager_memory_max_gb
    (a larger TM can run more chained operators / statements). CPU-bound floor still applies.
    """
    min_tm_mb = max(1, int(round(input_params.taskmanager_memory_min_gb * 1024)))
    max_tm_mb = max(min_tm_mb, int(round(input_params.taskmanager_memory_max_gb * 1024)))
    tm_cpu_cap = input_params.taskmanager_cpu_max
    taskmanager_cpu_cores = min(
        tm_cpu_cap,
        max(MIN_TASKMANAGER_CPU_CORES, total_cpu_cores // 2),
    )
    tm_count_for_cpu = max(1, math.ceil(total_cpu_cores / taskmanager_cpu_cores))
    # Fewest TMs that could fit memory if each TM is allowed up to max_tm_mb
    tm_count_for_memory = max(1, math.ceil(total_memory_mb / max_tm_mb))
    num_taskmanagers = max(tm_count_for_cpu, tm_count_for_memory)
    taskmanager_memory_mb = min(
        max_tm_mb,
        max(min_tm_mb, math.ceil(total_memory_mb / num_taskmanagers)),
    )
    # If cap prevents fitting (rare), add TMs until aggregate memory covers the estimate
    while num_taskmanagers * taskmanager_memory_mb < total_memory_mb:
        num_taskmanagers += 1
        taskmanager_memory_mb = min(
            max_tm_mb,
            max(min_tm_mb, math.ceil(total_memory_mb / num_taskmanagers)),
        )
        if num_taskmanagers > 10_000:
            break
    return num_taskmanagers, taskmanager_memory_mb, taskmanager_cpu_cores


def _compute_scaling_recommendations(
    input_params: EstimationInput,
    total_cpu_cores: int,
    processing_load: float,
) -> ScalingRecommendations:
    if input_params.data_skew_risk == "high":
        min_parallelism = max(1, total_cpu_cores // 3)
        recommended_parallelism = max(
            total_cpu_cores // 2,
            min(input_params.num_distinct_keys // 1000, total_cpu_cores),
        )
        max_parallelism = total_cpu_cores
        base_checkpoint_interval = min(60000, max(10000, 15000 + int(processing_load * 1000)))
    elif input_params.data_skew_risk == "medium":
        min_parallelism = max(1, total_cpu_cores // 2)
        recommended_parallelism = min(
            total_cpu_cores,
            max(total_cpu_cores // 2, input_params.num_distinct_keys // 2000),
        )
        max_parallelism = total_cpu_cores * 2
        base_checkpoint_interval = min(60000, max(5000, 10000 + int(processing_load * 1000)))
    else:
        min_parallelism = max(1, total_cpu_cores // 2)
        recommended_parallelism = total_cpu_cores
        max_parallelism = total_cpu_cores * 2
        base_checkpoint_interval = min(60000, max(5000, 8000 + int(processing_load * 1000)))

    if input_params.expected_latency_seconds <= 0.5:
        checkpoint_interval = min(base_checkpoint_interval, 5000)
    elif input_params.expected_latency_seconds <= 1.0:
        checkpoint_interval = min(base_checkpoint_interval, 10000)
    elif input_params.expected_latency_seconds <= 5.0:
        checkpoint_interval = min(base_checkpoint_interval, 20000)
    else:
        checkpoint_interval = base_checkpoint_interval

    if input_params.expected_latency_seconds <= 1.0:
        parallelism_boost = max(1, int(2.0 / input_params.expected_latency_seconds))
        recommended_parallelism = min(max_parallelism, recommended_parallelism * parallelism_boost)

    return ScalingRecommendations(
        min_parallelism=min_parallelism,
        recommended_parallelism=recommended_parallelism,
        max_parallelism=max_parallelism,
        checkpointing_interval_ms=checkpoint_interval,
    )


def calculate_flink_estimation(input_params: EstimationInput) -> EstimationResult:
    """
    Calculate Flink resource estimation based on input parameters.

    Args:
        input_params: Validated input parameters for the estimation

    Returns:
        EstimationResult: Complete estimation with resource recommendations
    """
    SIMPLE_RPS= 24000 
    MEDIUM_RPS= 11000
    COMPLEX_RPS= 2500
    total_cpu_cores=1
    total_memory_mb=8
    total_nodes=2
    processing_load = 1
    jobmanager_memory_mb = 1024
    jobmanager_cpu_cores = JOBMANAGER_CPU_CORES
    
    total_throughput_mb_per_sec = input_params.total_throughput_mb_per_sec
    
    simple_statement_cpu_needs = min(MIN_TASKMANAGER_CPU_CORES,  total_throughput_mb_per_sec / (SIMPLE_RPS* input_params.avg_record_size_bytes)* input_params.simple_statements)
    num_taskmanagers = max(1, simple_statement_cpu_needs )
    taskmanager_memory_mb = max(1024, min(4096, int(total_memory_mb // 8)))
    input_summary = InputSummary(
        messages_per_second=input_params.messages_per_second,
        avg_record_size_bytes=input_params.avg_record_size_bytes,
        total_throughput_mb_per_sec=round(total_throughput_mb_per_sec, 2),
        num_distinct_keys=input_params.num_distinct_keys,
        data_skew_risk=input_params.data_skew_risk,
        bandwidth_capacity_mbps=input_params.bandwidth_capacity_mbps,
        expected_latency_seconds=input_params.expected_latency_seconds,
        simple_statements=input_params.simple_statements,
        medium_statements=input_params.medium_statements,
        complex_statements=input_params.complex_statements,
        total_statements=input_params.total_statements,
    )

    resource_estimates = ResourceEstimates(
        total_memory_mb=math.ceil(total_memory_mb),
        total_cpu_cores=total_cpu_cores,
        total_nodes=total_nodes,
        processing_load_score=round(processing_load, 2),
    )

    jobmanager_config = JobManagerConfig(
        memory_mb=math.ceil(jobmanager_memory_mb),
        cpu_cores=float(jobmanager_cpu_cores),
    )

    taskmanager_config = TaskManagerConfig(
        count=num_taskmanagers,
        memory_mb_each=math.ceil(taskmanager_memory_mb),
        cpu_cores_each=taskmanager_cpu_cores,
        total_memory_mb=num_taskmanagers * math.ceil(taskmanager_memory_mb),
        total_cpu_cores=num_taskmanagers * taskmanager_cpu_cores,
    )

    cluster_recommendations = ClusterRecommendations(
        jobmanager=jobmanager_config,
        taskmanagers=taskmanager_config,
    )

    scaling_recommendations = _compute_scaling_recommendations(
        input_params,
        total_cpu_cores,
        processing_load,
    )

    result = EstimationResult(
        input_summary=input_summary,
        resource_estimates=resource_estimates,
        cluster_recommendations=cluster_recommendations,
        scaling_recommendations=scaling_recommendations,
    )

    if os.environ.get("FLINK_ESTIMATOR_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        print("-" * 80)
        print(result.model_dump_json(indent=2))
        print("-" * 80)

    return result



def calculate_flink_estimation_v0(input_params: EstimationInput) -> EstimationResult:
    """
    Calculate Flink resource estimation based on input parameters.

    Args:
        input_params: Validated input parameters for the estimation

    Returns:
        EstimationResult: Complete estimation with resource recommendations
    """
    total_throughput_mb_per_sec = input_params.total_throughput_mb_per_sec
    skew_multipliers = {
        "low": LOW_SKEW_FACTOR,
        "medium": MEDIUM_SKEW_FACTOR,
        "high": HIGH_SKEW_FACTOR,
    }
    skew_factor = skew_multipliers.get(input_params.data_skew_risk, MEDIUM_SKEW_FACTOR)

    bandwidth_cpu_penalty = _bandwidth_cpu_penalty(
        total_throughput_mb_per_sec,
        input_params.bandwidth_capacity_mbps,
    )
    processing_load = _compute_processing_load(input_params)
    total_memory_mb, _ = _compute_memory_mb(
        input_params,
        total_throughput_mb_per_sec,
        processing_load,
        skew_factor,
    )
    total_cpu_cores = _compute_cpu_cores(
        input_params,
        total_throughput_mb_per_sec,
        processing_load,
        bandwidth_cpu_penalty,
        skew_factor,
    )
    total_nodes = _compute_total_nodes(total_cpu_cores)

    num_taskmanagers, taskmanager_memory_mb, taskmanager_cpu_cores = _compute_taskmanager_layout(
        input_params,
        total_memory_mb,
        total_cpu_cores,
    )

    jobmanager_memory_mb = max(1024, min(4096, int(total_memory_mb // 8)))
    jobmanager_cpu_cores = JOBMANAGER_CPU_CORES

    input_summary = InputSummary(
        messages_per_second=input_params.messages_per_second,
        avg_record_size_bytes=input_params.avg_record_size_bytes,
        total_throughput_mb_per_sec=round(total_throughput_mb_per_sec, 2),
        num_distinct_keys=input_params.num_distinct_keys,
        data_skew_risk=input_params.data_skew_risk,
        bandwidth_capacity_mbps=input_params.bandwidth_capacity_mbps,
        expected_latency_seconds=input_params.expected_latency_seconds,
        simple_statements=input_params.simple_statements,
        medium_statements=input_params.medium_statements,
        complex_statements=input_params.complex_statements,
        total_statements=input_params.total_statements,
    )

    resource_estimates = ResourceEstimates(
        total_memory_mb=math.ceil(total_memory_mb),
        total_cpu_cores=total_cpu_cores,
        total_nodes=total_nodes,
        processing_load_score=round(processing_load, 2),
    )

    jobmanager_config = JobManagerConfig(
        memory_mb=math.ceil(jobmanager_memory_mb),
        cpu_cores=float(jobmanager_cpu_cores),
    )

    taskmanager_config = TaskManagerConfig(
        count=num_taskmanagers,
        memory_mb_each=math.ceil(taskmanager_memory_mb),
        cpu_cores_each=taskmanager_cpu_cores,
        total_memory_mb=num_taskmanagers * math.ceil(taskmanager_memory_mb),
        total_cpu_cores=num_taskmanagers * taskmanager_cpu_cores,
    )

    cluster_recommendations = ClusterRecommendations(
        jobmanager=jobmanager_config,
        taskmanagers=taskmanager_config,
    )

    scaling_recommendations = _compute_scaling_recommendations(
        input_params,
        total_cpu_cores,
        processing_load,
    )

    result = EstimationResult(
        input_summary=input_summary,
        resource_estimates=resource_estimates,
        cluster_recommendations=cluster_recommendations,
        scaling_recommendations=scaling_recommendations,
    )

    if os.environ.get("FLINK_ESTIMATOR_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        print("-" * 80)
        print(result.model_dump_json(indent=2))
        print("-" * 80)

    return result


def save_estimation_to_json(
    input_params: EstimationInput,
    estimation_result: EstimationResult,
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

    estimation_id = str(uuid.uuid4())[:8]
    timestamp = datetime.now().isoformat()

    metadata = EstimationMetadata(
        estimation_id=estimation_id,
        timestamp=timestamp,
        project_name=input_params.project_name,
        saved_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )

    saved_estimation = SavedEstimation(
        metadata=metadata,
        input_parameters=input_params,
        estimation_results=estimation_result,
    )

    safe_project_name = "".join(
        c for c in input_params.project_name if c.isalnum() or c in (" ", "-", "_")
    ).rstrip()
    safe_project_name = safe_project_name.replace(" ", "_")
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_project_name}_{timestamp_str}_{estimation_id}.json"
    filepath = os.path.join(SAVED_ESTIMATIONS_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(saved_estimation.model_dump_json(indent=2))

    return filename


def get_saved_estimations_directory() -> str:
    """
    Get the directory path where estimations are saved.

    Returns:
        str: Path to the saved estimations directory
    """
    return SAVED_ESTIMATIONS_DIR


os.makedirs(SAVED_ESTIMATIONS_DIR, exist_ok=True)
