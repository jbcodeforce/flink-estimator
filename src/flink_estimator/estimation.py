"""
Flink resource estimation calculation logic.

This module contains the core business logic for calculating
Flink resource requirements and file persistence operations.

Public API: ``calculate_flink_estimation``, ``save_estimation_to_json``,
``get_saved_estimations_directory``. Other callables are module-private (``_`` prefix).

So rule of thumb:
Flink can process 24000 records per second per core for simple statements, 11000 for medium statements, and 2500 for complex statements.
But those numbers may go lower with bigger messages, bigger state, key skew, number of disctint keys, etc.

Statement complexity reflect the usage of complex operators, like joins, windowed aggregations, etc.

Source and Sink latency are not considered and assumed to be minimal.
Assumed the key size is minimal, a few bytes

A Flink job is composed of a graph of operators, Operators are deployed, chained in threads, and executed in task managers with a configured parallelism
* Each Operator has its own costs
* Working State is kept locally in RocksDB impacting storage requirements
* But is backed up and restored remotely to distributed storage impacting checkpoint size and latency
* Depends on operator structure and settings and message size
* Checkpoint copies state for recovery
* Checkpoint Interval determines how frequently state is captured
* Aggregate State Size consumes bandwidth, determines recovery time which impacts latency

The minimum number of nodes for Flink is 3 for HA but the estimator can propose 1 taskmanager and 1 job manager for minimum..


CPU means an actual bare metal processing unit that has at least one CPU Core. Multi-core or hyperthreading processors are counted as one CPU.
A CPU Core refers to "cpu units" in Kubernetes. 1 CPU is equivalent to one AWS vCPU, 1 GCP core, 1 Azure vCore or 1 hyperthread on a bare-metal processor with hyperthreading enabled.

Consider a Flink node (running task manager) to be a 4 CPU node, with 16GB of memory. 
They should process 20 to 50 MB/s of data.
Typical Flink jobs have a lot of state and benefit for more memory.

Think to scale vertically before horizontally.

Heuristics:
* increase the number of task managers until there is enough resource to get the expected 
throughput or memory size to keep state

total_nodes is a coarse count from total CPU and an 8-cores-per-node assumption (minimum 3). 

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


JOBMANAGER_CPU_CORES = 0.5  # Minimum viable JM CPU (Kubernetes cpu units)
NB_TM_PER_JM = 24   # number of task managers per job manager
MIN_TASKMANAGER_CPU_CORES = 0.5
NB_CPU_PER_NODE = 8
NB_CPU_PER_TM = 8

SIMPLE_RPS= 24000 
MEDIUM_RPS= 11000
COMPLEX_RPS= 2500



def _latency_cpu_factor(expected_latency_seconds: float) -> float:
    if expected_latency_seconds <= 0.5:
        return 1.5
    if expected_latency_seconds <= 1.0:
        return 1.2
    if expected_latency_seconds <= 5.0:
        return 1.1
    return 1.0



def _compute_total_nodes(total_cpu_cores: float) -> int:
    return max(1, math.ceil(total_cpu_cores / NB_CPU_PER_NODE))


def _compute_taskmanager_layout(
    input_params: EstimationInput,
    total_memory_mb: float,
    total_cpu_cores: int,
) -> tuple[int, int, int]:
    """
    Size TaskManagers so total memory and CPU fit.

    Memory-bound: use as few TMs as possible by filling each up to taskmanager_memory_gb
    (a larger TM can run more chained operators / statements). CPU-bound floor still applies.
    """
    min_tm_mb = max(1, int(round(input_params.taskmanager_memory_min_gb * 1024)))
    max_tm_mb = max(min_tm_mb, int(round(input_params.taskmanager_memory_gb * 1024)))
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


def _assess_cpu_need_for_throughput(total_throughput_mb_per_sec, input_params: EstimationInput):
    simple_throughput_mbps = SIMPLE_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    simple_statement_cpu_needs = max(MIN_TASKMANAGER_CPU_CORES,  total_throughput_mb_per_sec / simple_throughput_mbps * input_params.simple_statements)
    
    medium_throughput_mbps = MEDIUM_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    medium_statement_cpu_needs = max(MIN_TASKMANAGER_CPU_CORES, total_throughput_mb_per_sec /  medium_throughput_mbps * input_params.medium_statements)
    
    complex_throughput_mbps = COMPLEX_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    complex_statement_cpu_needs = max(MIN_TASKMANAGER_CPU_CORES,  total_throughput_mb_per_sec/ complex_throughput_mbps * input_params.complex_statements)
    
    total_cpu_needs = simple_statement_cpu_needs + medium_statement_cpu_needs + complex_statement_cpu_needs
    return total_cpu_needs


def _assess_cpu_need_due_to_memory(input_params: EstimationInput):
    state_size_GB = input_params.num_distinct_keys * input_params.avg_record_size_bytes / (1024 * 1024 * 1024)
    mem_need= state_size_GB * (input_params.medium_statements + input_params.complex_statements)
    return mem_need/(input_params.taskmanager_memory_gb), math.ceil(mem_need)


def calculate_flink_estimation(input_params: EstimationInput) -> EstimationResult:
    """
    Calculate Flink resource estimation based on input parameters.

    Args:
        input_params: Validated input parameters for the estimation

    Returns:
        EstimationResult: Complete estimation with resource recommendations
    """

   
    processing_load = 1 # not used yet 
    
    

    total_throughput_mb_per_sec = input_params.total_throughput_mb_per_sec
    

    total_cpu_need_for_throuput = _assess_cpu_need_for_throughput(total_throughput_mb_per_sec, input_params)

    cpu_need_due_to_memory, mem_need_for_state_GB = _assess_cpu_need_due_to_memory(input_params)
    total_cpu_needs= max(total_cpu_need_for_throuput, cpu_need_due_to_memory)
    num_taskmanagers = math.ceil(total_cpu_needs)
    jobmanager_cpus = max(JOBMANAGER_CPU_CORES, math.ceil(num_taskmanagers / NB_TM_PER_JM))
    taskmanager_memory_mb = input_params.taskmanager_memory_gb * 1024 * math.ceil(num_taskmanagers)
    jobmanager_memory_mb = 1024 * math.ceil(jobmanager_cpus)
    total_memory_mb = taskmanager_memory_mb + jobmanager_memory_mb

    total_cpus= math.ceil(jobmanager_cpus + total_cpu_needs)
    total_nodes= _compute_total_nodes(total_cpus)

    taskmanager_config = TaskManagerConfig(
        count=num_taskmanagers,
        memory_gb_each=math.ceil(taskmanager_memory_mb),
        total_memory_mb=num_taskmanagers * math.ceil(taskmanager_memory_mb),
        total_cpus=math.ceil(total_cpu_needs),
    )
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
        tm_memory_capacity_gb=input_params.taskmanager_memory_gb

    )

    resource_estimates = ResourceEstimates(
        total_memory_mb=math.ceil(total_memory_mb),
        total_cpus=math.ceil(total_cpus),
        total_nodes=math.ceil(total_nodes),
        processing_load_score=round(processing_load, 2),
    )

    jobmanager_config = JobManagerConfig(
        memory_mb=math.ceil(jobmanager_memory_mb),
        cpu_cores=float(jobmanager_cpus),
    )

    taskmanager_config = TaskManagerConfig(
        count=num_taskmanagers,
        memory_gb_each=math.ceil(input_params.taskmanager_memory_gb),
        total_memory_mb= math.ceil(taskmanager_memory_mb),
        total_cpus=math.ceil(total_cpu_needs),
    )

    cluster_recommendations = ClusterRecommendations(
        jobmanager=jobmanager_config,
        taskmanagers=taskmanager_config,
    )

    scaling_recommendations = _compute_scaling_recommendations(
        input_params,
        math.ceil(total_cpu_needs),
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
