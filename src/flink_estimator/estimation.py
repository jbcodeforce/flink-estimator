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

The estimator work for container based deployments. The memory constraints are for total process memory, but state
is kept in RockDB. With HashMapStateBackend Flink holds data internally as objects on the Java heap.
The EmbeddedRocksDBStateBackend holds in-flight data in a RocksDB database that is (per default) stored in the TaskManager local data directories.
The amount of state that you can keep is only limited by the amount of disk space available.

Therefore it is not relevant to consider memory footprint of state for the estimator.

For k8s deployments, the minimum number of worker nodes for Flink is 3 for HA.

CPU means an actual bare metal processing unit that has at least one CPU Core. Multi-core or hyperthreading processors are counted as one CPU.
A CPU Core refers to "cpu units" in Kubernetes. 1 CPU is equivalent to one AWS vCPU, 1 GCP core, 1 Azure vCore or 1 hyperthread on a bare-metal 
processor with hyperthreading enabled.

Consider a Flink node (running task managers) to be a 4 CPU node, with 16GB of memory. It should
be able to run 3 task managers, as default configuration is 1 core and 4 GB of memory per task manager.
They should process 20 to 50 MB/s of data.

Think to scale vertically before horizontally.

Heuristics:
* increase the number of task managers until there is enough resource to get the expected 
throughput or memory size to keep state

total_nodes is a coarse count from total CPU and an 8-cores-per-node assumption (minimum 3). 

"""

import math
import os
from re import L
import uuid
from datetime import datetime
import logging

LOGS_DIR = "logs"
os.makedirs(LOGS_DIR, exist_ok=True)
# Do not use logging.basicConfig(): it is a no-op if the root logger already
# has handlers (e.g. from uvicorn/FastAPI/Starlette), so the log file is never
# created. A dedicated file handler on this module's logger is always applied.
_log_file = os.path.join(LOGS_DIR, "flink_estimator.log")
logger = logging.getLogger(__name__)
if not any(
    isinstance(h, logging.FileHandler) and os.path.abspath(getattr(h, "baseFilename", "")) == os.path.abspath(_log_file)
    for h in logger.handlers
):
    _fh = logging.FileHandler(_log_file, encoding="utf-8")
    _fh.setLevel(logging.INFO)
    _fh.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logger.addHandler(_fh)
    logger.setLevel(logging.INFO)

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
    CapacityAnalysis,
    VM_TSHIRT_MB_CPU
)

# Configuration
SAVED_ESTIMATIONS_DIR = "saved_estimations"

# Host = worker node parameters
NB_CPU_PER_NODE = 8
OS_MEM_MB = 512

# record per second per core per statement type
SIMPLE_RPS= 24000 
MEDIUM_RPS= 11000
COMPLEX_RPS= 2500

# Flink Task Manager and job manager parameters
JOBMANAGER_MEM_MB = 2048
JOBMANAGER_CPU_CORES = 1  # Minimum viable JM CPU (Kubernetes cpu units) for 9 TM.
JM_TSHIRT_MB_CPU = {
    "S": (1,2048),
    "M": (2,4096),
    "L": (4,8192)
}

TM_MEM_MB = 4096  # Task Manager total process memory size in MB
TM_JVM_OVERHEAD_MB = 512 # Task Manager JVM overhead memory size in MB
TM_MM_PERCENT = 0.4 # percentage of flink process memory allocate to state
NB_TM_PER_JM = 24   # number of task managers per job manager
TM_vCPUs = 4



def calculate_flink_estimation(input_params: EstimationInput) -> EstimationResult:
    """
    Calculate Flink resource estimation based on input parameters.
    The number of task manager is a function of state size and throughput.
    Args:
        input_params: Validated input parameters for the estimation

    Returns:
        EstimationResult: Complete estimation with resource recommendations, and capacity analysis
        - input_summary: Summary of the input parameters
        - resource_estimates: Estimated resource requirements
        - cluster_recommendations: Cluster configuration recommendations
        - scaling_recommendations: Scaling and performance recommendations
        - capacity_analysis: Capacity analysis
    """
    # defaulting input parameters in case some are missing
    input_params = _defaulting_input_params(input_params)
    logger.info("input_params: %s", input_params.model_dump_json(indent=2))
    total_throughput_mb_per_sec = input_params.total_throughput_mb_per_sec
    jm_cpu,jm_memory = _assess_jobmanager_size(input_params)
    num_taskmanagers, node_allocations, num_jm = _assess_nb_taskmanagers(input_params, jm_memory)

    total_cpu_need_for_throughput, nb_worker_nodes = _assess_cpu_needs(total_throughput_mb_per_sec, input_params, jm_cpu)
    logger.info("total_cpu_need_for_throughput: %s", total_cpu_need_for_throughput)
    # State or throughput drive CPU count
    total_cpu_needs= max(total_cpu_need_for_throughput, num_taskmanagers* TM_vCPUs)
    taskmanager_memory_mb = num_taskmanagers * TM_MEM_MB
    total_memory_mb = taskmanager_memory_mb + jm_memory * num_jm
    total_nodes = max(nb_worker_nodes, len(node_allocations))

    gbps = input_params.bandwidth_capacity_gbps
    bandwidth_mbps = int(round(gbps * 1000))

    input_summary = InputSummary(
        messages_per_second=input_params.messages_per_second,
        avg_record_size_bytes=input_params.avg_record_size_bytes,
        total_throughput_mb_per_sec=round(total_throughput_mb_per_sec, 2),
        num_distinct_keys=input_params.num_distinct_keys,
        data_skew_risk=input_params.data_skew_risk,
        bandwidth_capacity_mbps=bandwidth_mbps,
        expected_latency_seconds=input_params.expected_latency_seconds,
        simple_statements=input_params.simple_statements,
        medium_statements=input_params.medium_statements,
        complex_statements=input_params.complex_statements,
        total_statements=input_params.total_statements,
        worker_node_memory_capacity_mb=float(input_params.worker_node_memory_mb),
        worker_node_cpu_capacity=input_params.worker_node_cpu_max,
        nb_worker_nodes=input_params.nb_worker_nodes,
        worker_node_t_size=input_params.worker_node_t_size,
    )

    # Placeholder until processing_load is fully wired; matches scaling checkpoint heuristic seed.
    processing_load_score = 1.0
    resource_estimates = ResourceEstimates(
        total_memory_mb=math.ceil(total_memory_mb),
        total_cpus=math.ceil(total_cpu_needs),
        total_nodes=math.ceil(total_nodes),
        processing_load_score=processing_load_score,
    )

    jobmanager_config = JobManagerConfig(
        count=num_jm,
        memory_mb=math.ceil(jm_memory),
        cpu_cores=float(jm_cpu),
    )

    tm_count = num_taskmanagers
    tm_total_mem = math.ceil(taskmanager_memory_mb)
    memory_gb_each = (
        round((tm_total_mem / tm_count) / 1024, 2) if tm_count else 0.0
    )
    taskmanager_config = TaskManagerConfig(
        count=tm_count,
        total_memory_mb=tm_total_mem,
        total_cpus=math.ceil(total_cpu_needs - jm_cpu),
        memory_gb_each=memory_gb_each,
    )

    cluster_recommendations = ClusterRecommendations(
        jobmanager=jobmanager_config,
        taskmanagers=taskmanager_config,

    )

    scaling_recommendations = _compute_scaling_recommendations(
        input_params,
       taskmanager_config.total_cpus
    )

    capacity_analysis = CapacityAnalysis(
        total_flink_statements = input_params.total_statements * input_params.number_flink_applications,
        total_flink_applications = input_params.number_flink_applications
    )
    result = EstimationResult(
        input_summary=input_summary,
        resource_estimates=resource_estimates,
        cluster_recommendations=cluster_recommendations,
        scaling_recommendations=scaling_recommendations,
        capacity_analysis=capacity_analysis,
    )
    logger.info("result: %s", result.model_dump_json(indent=2))
    if os.environ.get("FLINK_ESTIMATOR_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        print("-" * 80)
        print(result.model_dump_json(indent=2))
        print("-" * 80)

    return result


def _latency_cpu_factor(expected_latency_seconds: float) -> float:
    if expected_latency_seconds <= 0.5:
        return 1.5
    if expected_latency_seconds <= 1.0:
        return 1.2
    if expected_latency_seconds < 5.0:
        return 1.1
    return 1.0


def _assess_jobmanager_size(input_params: EstimationInput) -> tuple[int, int]:
    if input_params.num_distinct_keys <= 10000000: # 10 million keys
        jm_tshirt_size ='S'
    elif input_params.num_distinct_keys <= 100000000: # 100 million keys
        jm_tshirt_size ='M'
    else:
        jm_tshirt_size ='L'
    return JM_TSHIRT_MB_CPU[jm_tshirt_size][0], JM_TSHIRT_MB_CPU[jm_tshirt_size][1]


def _greedy_pack_taskmanagers(
    free_mem_per_node: list[int],
    nb_taskmanagers: int,
    tm_mem_mb: int,
) -> tuple[list[int], int, bool, list[int]]:
    """
    Place nb_taskmanagers TMs, each using tm_mem_mb, on the worker with the most
    remaining memory that can still fit a TM. Tie-break: lower node index.

    Returns:
        allocations: TMs per worker node (len == number of nodes)
        max_on_a_node: max TMs on any single node after packing
        success: True if all TMs were placed
    """
    n = len(free_mem_per_node)
    alloc: list[int] = [0] * n
    remaining = list(free_mem_per_node)
    for _ in range(nb_taskmanagers):
        best_i = -1
        best_rem = -1
        for i in range(n):
            r = remaining[i]
            if r >= tm_mem_mb and r > best_rem:
                best_rem = r
                best_i = i
        if best_i < 0:
            return alloc, (max(alloc) if alloc else 0), False, remaining
        remaining[best_i] -= tm_mem_mb
        alloc[best_i] += 1
    return alloc, max(alloc), True, remaining


def _assess_nb_taskmanagers(input_params: EstimationInput, jm_memory: int) -> tuple[int, list[int], int]:
    """
    Assess the number of task managers needed based on state size and expected throughput.
    Args:
        input_params: The input parameters used for estimation
    Returns:
        num_taskmanagers: The number of task managers needed across all worker nodes
        max_tm_per_node: After greedy placement, the largest number of TMs on any
            one worker node (0 if placement failed, which should not happen).
        num_jm: The number of job managers needed
    """
    # potentially more than memory of one task manager
    total_flink_process_mem = math.ceil(input_params.num_distinct_keys 
                                            * (input_params.medium_statements + input_params.complex_statements) 
                                            * input_params.number_flink_applications
                                            * input_params.avg_record_size_bytes  
                                            / (1024 * 1024)
                                            )
    total_flink_process_mem = total_flink_process_mem / TM_MM_PERCENT
    logger.info("total_flink_process_mem: %s MB", total_flink_process_mem)
    # Can we have enough memory with the current worknodes, and can we place each TM?
    free_mem_per_node: list[int] = []
    total_free_mem = 0.0
    total_mem_needed_mb = max(TM_MEM_MB, total_flink_process_mem)
    nb_taskmanagers = max(1, math.ceil(total_mem_needed_mb / TM_MEM_MB))
    nb_jm = max(1, math.ceil(nb_taskmanagers / NB_TM_PER_JM))
    jm_memory = nb_jm * jm_memory
    logger.info("jm_memory: %s MB", jm_memory)
    while True:
        free_mem_per_node, total_free_mem = _assess_free_mem_per_node(input_params, jm_memory)
        if not free_mem_per_node or max(free_mem_per_node) < TM_MEM_MB:
            raise ValueError(
                f"No worker can host a {TM_MEM_MB} MB task manager: "
                f"max free memory on a node is {max(free_mem_per_node) if free_mem_per_node else 0} MB. "
                "Increase worker_node_memory (or use a larger VM t-shirt) so each node can hold at least one TM."
            )
        if total_free_mem < total_mem_needed_mb:
            input_params.nb_worker_nodes += 1
            continue
       
        tm_allocations_wnodes, max_tm_per_node, ok, free_mem_per_node = _greedy_pack_taskmanagers(
            free_mem_per_node, nb_taskmanagers, TM_MEM_MB
        )
        if ok:
            break
        logger.warning(
            "Could not place %s task managers of %s MB; adding a worker node",
            nb_taskmanagers,
            TM_MEM_MB,
        )
        input_params.nb_worker_nodes += 1

    logger.info("total_mem_needed_mb: %s MB", total_mem_needed_mb)
    logger.info("free_mem_per_node: %s", free_mem_per_node)
    logger.info("nb_taskmanagers: %s", nb_taskmanagers)
    logger.info("tm_allocations_wnodes: %s", tm_allocations_wnodes)
    logger.info("max_tm_per_node (largest count on one node): %s", max_tm_per_node)
    return nb_taskmanagers, tm_allocations_wnodes, nb_jm

def _assess_free_mem_per_node(input_params: EstimationInput, jm_memory: int) -> tuple[list[int], int]:
    free_mem_per_node = []
    total_mem = 0
    for wnode in range(input_params.nb_worker_nodes):
         free_mem_per_node.append(input_params.worker_node_memory_mb - OS_MEM_MB)
         total_mem += free_mem_per_node[wnode]
    free_mem_per_node[0] = free_mem_per_node[0] - jm_memory * input_params.number_flink_applications
    total_mem = total_mem - jm_memory * input_params.number_flink_applications
    logger.info("free_mem_per_node: %s MB", free_mem_per_node)
    logger.info("total_free_mem: %s MB", total_mem)
    return free_mem_per_node, total_mem




def _compute_scaling_recommendations(
    input_params: EstimationInput,
    total_cpu_cores: int    
) -> ScalingRecommendations:
    """
    Compute the scaling recommendations based on the total CPU cores and the data skew risk.
    Args:
        input_params: The input parameters used for estimation
        total_cpu_cores: The total number of CPU cores needed
    Returns:
        ScalingRecommendations: The scaling recommendations
    """
    processing_load = 1 # not used yet 
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


def _assess_cpu_needs(total_throughput_mb_per_sec, input_params: EstimationInput, jm_cpu: int) -> tuple[int, int]:  
    """
    Assess the number of CPU cores needed based on the total throughput and the expected latency.
    Args:
        total_throughput_mb_per_sec: The total throughput in MB per second
        input_params: The input parameters used for estimation
        jm_cpu: The number of CPU cores needed for the job manager
    Returns:
        total_cpu_needs: The total number of CPU cores needed
        nb_worker_nodes: The number of worker nodes needed
    """
    simple_throughput_mbps = SIMPLE_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    simple_statement_cpu_needs = max(TM_vCPUs,  total_throughput_mb_per_sec / simple_throughput_mbps) * input_params.simple_statements
    
    medium_throughput_mbps = MEDIUM_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    medium_statement_cpu_needs = max(TM_vCPUs, total_throughput_mb_per_sec /  medium_throughput_mbps) * input_params.medium_statements
    
    complex_throughput_mbps = COMPLEX_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    complex_statement_cpu_needs = max(TM_vCPUs,  total_throughput_mb_per_sec/ complex_throughput_mbps) * input_params.complex_statements
    
    total_cpu_needs = math.ceil((simple_statement_cpu_needs + medium_statement_cpu_needs + complex_statement_cpu_needs) * _latency_cpu_factor(input_params.expected_latency_seconds)
                      + jm_cpu) * input_params.number_flink_applications
    logger.info("total_cpu_needs: %s", total_cpu_needs)
    nb_worker_nodes = input_params.nb_worker_nodes
    if input_params.worker_node_type == "VM" and input_params.worker_node_t_size is not None:
        cores_per_node = VM_TSHIRT_MB_CPU[input_params.worker_node_t_size][1]
    else:
        cores_per_node = input_params.worker_node_cpu_max
    while True:
        cpu_capacity_cross_nodes = nb_worker_nodes * cores_per_node
        if cpu_capacity_cross_nodes >= total_cpu_needs:
            break
        nb_worker_nodes += 1
    return total_cpu_needs, nb_worker_nodes



def _defaulting_input_params(input_params: EstimationInput) -> EstimationInput:
    if input_params.worker_node_type == "VM":
        input_params.worker_node_memory_mb = VM_TSHIRT_MB_CPU[input_params.worker_node_t_size][0]
        input_params.worker_node_cpu_max = VM_TSHIRT_MB_CPU[input_params.worker_node_t_size][1]

    return input_params




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
