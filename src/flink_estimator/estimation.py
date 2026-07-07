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

Sizing model (three independent dimensions; 
1. CPU (PRIMARY) — throughput-driven and uncapped: cores = Σ(throughput / per-core-rate × stmts),
   scaled by a latency factor, per statement type, plus JobManager cores. State does NOT add cores.
   total_cores = TaskManager cores + JobManager cores. A "CP Flink node" is 8 cores, so the headline
   output is cp_flink_nodes = ⌈total_cores / 8⌉. Cores are fractional and freely allocatable to TMs.

2. Local disk (state) — under the EmbeddedRocksDBStateBackend (recommended default) keyed state lives
   on the TaskManager's local NVMe disk, NOT in RAM; only a bounded managed-memory slice is cached and
   it does not scale with state size. state_size = keys × (medium+complex stmts) × record × apps;
   required disk = state_size × 1.5 (RocksDB compaction/WAL headroom). HashMapStateBackend (heap, RAM-
   bound) is not modeled — assume rocksdb.

3. RAM — TaskManager process memory: a configurable baseline (default 4 GB/TM) raised by a network/
   in-flight buffer heuristic when latency is tight. State is excluded (see #2).

Worker/VM nodes (SECONDARY) bin-pack RAM and local disk onto whole node shapes:
   worker_nodes = ⌈ max( RAM_total / usable_ram_per_node, required_disk / disk_per_node ) ⌉.
CPU never bounds the worker-node count. For packing efficiency, mem_per_tm should evenly divide a
node's usable RAM (otherwise RAM is stranded; reported in diagnostics).

Throughput rule of thumb: ~24000 simple / 11000 medium / 2500 complex records per second per core;
lower with bigger messages, bigger state, key skew, or many distinct keys. Key size assumed minimal.

CPU means a processing unit with at least one core; a CPU core maps to a Kubernetes "cpu unit"
(1 AWS vCPU / 1 GCP core / 1 Azure vCore / 1 hyperthread). For k8s HA, a real deployment uses >= 3
worker nodes. Scale vertically before horizontally.

"""

import math
import os
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
    SizingDiagnostics,
    VM_TSHIRT_MB_CPU
)

# Configuration
SAVED_ESTIMATIONS_DIR = "saved_estimations"

# Host = worker node parameters
OS_MEM_MB = 512

# record per second per core per statement type
SIMPLE_RPS= 24000 
MEDIUM_RPS= 11000
COMPLEX_RPS= 5500

# Flink Task Manager and job manager parameters
JOBMANAGER_MEM_MB = 2048
JOBMANAGER_CPU_CORES = 1  # Minimum viable JM CPU (Kubernetes cpu units) for 9 TM.
JM_TSHIRT_CPU_MB = {
    "S": (1,2048),
    "M": (2,4096),
    "L": (4,8192)
}

TM_MEM_MB = 4096  # Default Task Manager total process memory size in MB (overridable per input)
TM_PROCESS_MEMORY_MAX_MB = 64 * 1024  # ceiling for any single TaskManager process (MB)
IN_FLIGHT_TO_BUFFER = 0.4  # fraction of in-flight (throughput*latency) attributed to TM network/buffer memory
THROUGHPUT_BUFFER_K = 1.4  # MB per (MB/s per-TM) scaling for low-latency shuffles/serialization

# A "CP Flink node" is a normalized buy unit: a collection of 8 cores running CP Flink workloads.
# The primary output is the aggregate cores of all CP Flink workloads (TaskManagers + JobManagers)
# divided into 8-core nodes. Worker/VM nodes are a separate, shape-dependent bin-packing metric.
CP_FLINK_NODE_CORES = 8

# RocksDB keeps state on local NVMe disk. Raw state needs headroom for compaction, WAL, and SST
# overlap during merges; size local disk at state_size x this factor.
STATE_DISK_AMPLIFICATION = 1.5
GIB = 1024 ** 3


def _state_disk_gb(input_params: EstimationInput) -> tuple[float, int]:
    """
    Local NVMe disk required to hold keyed RocksDB state.

    Under the EmbeddedRocksDBStateBackend (recommended default) state lives on the TaskManager's
    local disk; only a bounded slice (Flink managed memory) is cached in RAM and that slice does
    not scale with total state size. So state size is a *disk* requirement, not a RAM requirement.

    Args:
        input_params: The input parameters used for estimation
    Returns:
        (state_size_gb, required_disk_gb) where required_disk_gb applies RocksDB compaction headroom.
    """
    state_size_gb = input_params.state_size_bytes / GIB
    required_disk_gb = math.ceil(state_size_gb * STATE_DISK_AMPLIFICATION)
    return state_size_gb, required_disk_gb


def _network_buffer_min_process_memory_mb(
    input_params: EstimationInput,
    total_throughput_mb_per_sec: float,
    nb_task_managers: int,
) -> int:
    """
    Heuristic for extra Flink process memory per TM (network, in-flight and shuffle buffers) when
    end-to-end latency is tight and/or per-TM throughput is high. Caller combines with state floor.
    """
    n = max(1, nb_task_managers)
    thr_per_tm = total_throughput_mb_per_sec / n
    lat = max(input_params.expected_latency_seconds, 0.01)
    lat_capped = min(lat, 30.0)
    in_flight_cluster = total_throughput_mb_per_sec * lat_capped
    in_flight_per_tm = in_flight_cluster / n
    if input_params.expected_latency_seconds <= 0.5:
        pressure = 2.0
    elif input_params.expected_latency_seconds <= 1.0:
        pressure = 1.5
    elif input_params.expected_latency_seconds < 5.0:
        pressure = 1.15
    else:
        pressure = 1.0
    rec = max(100, input_params.avg_record_size_bytes) / 1024.0
    rec_scale = 1.0 + 0.02 * min(rec, 64.0)
    pipe = max(1, input_params.total_statements)
    through_term = thr_per_tm * THROUGHPUT_BUFFER_K * rec_scale * pressure
    in_flight_term = IN_FLIGHT_TO_BUFFER * in_flight_per_tm
    extra = 256.0 * (1.0 + 0.01 * float(pipe))
    return max(0, int(math.ceil(through_term + in_flight_term + extra)))


def _per_tm_cap_mb(input_params: EstimationInput) -> int:
    """
    sets an upper bound (in MB) for a single Task Manager’s process memory when 
    the latency/buffer heuristic would otherwise suggest something huge.
    A TM cannot be larger than what can plausibly sit on one worker.
    TM is never sized above 64 GiB TM_PROCESS_MEMORY_MAX_MB, even on very large nodes.
    """
    w = int(input_params.worker_node_memory_mb)
    return min(TM_PROCESS_MEMORY_MAX_MB, max(w - int(OS_MEM_MB), TM_MEM_MB))


def _resolve_per_tm_memory_mb(
    input_params: EstimationInput,
    total_throughput_mb_per_sec: float,
    nb_taskmanagers: int,
) -> int:
    """
    Per-TaskManager process memory: the configured ``mem_per_tm_mb`` baseline, raised by the
    network/in-flight buffer heuristic when latency is tight, and capped at what fits on a worker.
    State is NOT included here — under RocksDB it lives on local disk, not RAM.
    """
    cap = _per_tm_cap_mb(input_params)
    buffer_mb = _network_buffer_min_process_memory_mb(
        input_params, total_throughput_mb_per_sec, nb_taskmanagers
    )
    return int(min(cap, max(input_params.mem_per_tm_mb, buffer_mb)))


def _pack_worker_nodes(
    input_params: EstimationInput,
    nb_taskmanagers: int,
    per_tm_mem_mb: int,
    jm_memory: int,
    required_disk_gb: int,
) -> dict:
    """
    Secondary metric: bin-pack TaskManager RAM and RocksDB local disk onto whole worker/VM nodes.
    CPU does not constrain node count (cores are fractional and freely allocatable to TMs).

    Returns a dict of packing facts used by both ResourceEstimates and SizingDiagnostics.
    """
    usable_ram_per_node = int(input_params.worker_node_memory_mb) - OS_MEM_MB
    if usable_ram_per_node < per_tm_mem_mb:
        raise ValueError(
            f"A worker node has {usable_ram_per_node} MB usable RAM, too small for a "
            f"{per_tm_mem_mb} MB TaskManager. Use a larger node shape or smaller mem_per_tm_mb."
        )
    tms_per_node = usable_ram_per_node // per_tm_mem_mb
    stranded_ram = usable_ram_per_node - tms_per_node * per_tm_mem_mb

    apps = max(1, input_params.number_flink_applications)
    # Place TaskManagers whole (accounts for per-TM stranding), then fit JobManager memory into
    # the RAM left unused on those nodes, adding nodes only if it does not fit.
    nodes_for_tms = math.ceil(nb_taskmanagers / tms_per_node)
    remaining_ram = nodes_for_tms * usable_ram_per_node - nb_taskmanagers * per_tm_mem_mb
    jm_demand = jm_memory * apps
    if jm_demand <= remaining_ram:
        ram_nodes = nodes_for_tms
    else:
        ram_nodes = nodes_for_tms + math.ceil((jm_demand - remaining_ram) / usable_ram_per_node)
    ram_nodes = max(1, ram_nodes)

    disk_per_node = int(input_params.worker_node_disk_gb)
    disk_nodes = math.ceil(required_disk_gb / disk_per_node) if required_disk_gb > 0 else 0

    # Worker-node count is the actual RAM/disk packing need; the requested nb_worker_nodes is an
    # informational echo on the summary, not a floor that forces over-provisioning.
    worker_nodes = max(1, ram_nodes, disk_nodes)
    if ram_nodes > disk_nodes:
        bounding = "ram"
    elif disk_nodes > ram_nodes:
        bounding = "disk"
    else:
        bounding = "balanced"

    ram_total_mb = nb_taskmanagers * per_tm_mem_mb + jm_memory * apps
    return {
        "worker_nodes": worker_nodes,
        "ram_nodes": ram_nodes,
        "disk_nodes": disk_nodes,
        "tms_per_node": tms_per_node,
        "stranded_ram_mb_per_node": stranded_ram,
        "ram_total_mb": ram_total_mb,
        "bounding": bounding,
    }


def _compute_sizing_diagnostics(
    tm_cores: float,
    jm_cores: float,
    nb_taskmanagers: int,
    per_tm_mem_mb: int,
    state_size_gb: float,
    required_disk_gb: int,
    packing: dict,
) -> SizingDiagnostics:
    """Surface the intermediate sizing values and which of RAM/disk bounds the worker-node count."""
    return SizingDiagnostics(
        tm_cores=round(tm_cores, 2),
        jm_cores=round(jm_cores, 2),
        nb_taskmanagers=nb_taskmanagers,
        mem_per_tm_mb=per_tm_mem_mb,
        ram_total_mb=int(packing["ram_total_mb"]),
        state_size_gb=round(state_size_gb, 2),
        required_disk_gb=required_disk_gb,
        tms_per_node=int(packing["tms_per_node"]),
        stranded_ram_mb_per_node=int(packing["stranded_ram_mb_per_node"]),
        ram_nodes=int(packing["ram_nodes"]),
        disk_nodes=int(packing["disk_nodes"]),
        worker_node_bounding_factor=packing["bounding"],
    )


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
    apps = max(1, input_params.number_flink_applications)
    jm_cpu, jm_memory = _assess_jobmanager_size(input_params)

    # --- PRIMARY: cores ---------------------------------------------------------------
    # CPU is throughput-driven (uncapped) for TaskManagers, plus JobManager cores. State does
    # not add cores. Cores are fractional and freely allocatable to TaskManagers.
    tm_cores = _throughput_cores(total_throughput_mb_per_sec, input_params)
    jm_cores = jm_cpu * apps
    total_cores = tm_cores + jm_cores
    cp_flink_nodes = max(1, math.ceil(total_cores / CP_FLINK_NODE_CORES))
    logger.info("tm_cores: %s, jm_cores: %s, total_cores: %s, cp_flink_nodes: %s",
                tm_cores, jm_cores, total_cores, cp_flink_nodes)

    # --- TaskManager shape ------------------------------------------------------------
    nb_task_managers = max(1, math.ceil(tm_cores / input_params.cores_per_tm))
    per_tm_mem_mb = _resolve_per_tm_memory_mb(
        input_params, total_throughput_mb_per_sec, nb_task_managers
    )

    # --- SECONDARY: state on local disk, RAM + disk bin-packed onto worker/VM nodes ----
    state_size_gb, required_disk_gb = _state_disk_gb(input_params)
    packing = _pack_worker_nodes(
        input_params, nb_task_managers, per_tm_mem_mb, jm_memory, required_disk_gb
    )
    logger.info("packing: %s", packing)

    gbps = input_params.bandwidth_capacity_gbps
    bandwidth_mbps = int(round(gbps * 1000))
    # From there build the report
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
    # Cores that physically come with the worker nodes provisioned for RAM/disk. Equals the compute
    # need on a balanced shape, but exceeds it when a RAM/disk-light shape forces extra nodes.
    provisioned_cores = packing["worker_nodes"] * int(input_params.worker_node_cpu_max)
    resource_estimates = ResourceEstimates(
        cp_flink_nodes=cp_flink_nodes,
        total_cpus=math.ceil(total_cores),
        provisioned_cores=provisioned_cores,
        total_memory_mb=int(packing["ram_total_mb"]),
        total_disk_gb=required_disk_gb,
        total_worker_node_needed=packing["worker_nodes"],
        processing_load_score=processing_load_score,
    )

    jobmanager_config = JobManagerConfig(
        count=apps,
        memory_mb=math.ceil(jm_memory),
        total_cpus=float(jm_cpu),
    )

    taskmanager_config = TaskManagerConfig(
        count=nb_task_managers,
        total_memory_mb=nb_task_managers * per_tm_mem_mb,
        total_cpus=math.ceil(tm_cores),
        memory_mb_each=float(per_tm_mem_mb),
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
        total_flink_statements = input_params.total_statements * apps,
        total_flink_applications = apps
    )
    sizing_diagnostics = _compute_sizing_diagnostics(
        tm_cores,
        jm_cores,
        nb_task_managers,
        per_tm_mem_mb,
        state_size_gb,
        required_disk_gb,
        packing,
    )
    result = EstimationResult(
        input_summary=input_summary,
        resource_estimates=resource_estimates,
        cluster_recommendations=cluster_recommendations,
        scaling_recommendations=scaling_recommendations,
        capacity_analysis=capacity_analysis,
        sizing_diagnostics=sizing_diagnostics,
    )
    logger.info("result: %s", result.model_dump_json(indent=2))
    if os.environ.get("FLINK_ESTIMATOR_DEBUG", "").strip().lower() in ("1", "true", "yes"):
        print("-" * 80)
        print(result.model_dump_json(indent=2))
        print("-" * 80)

    return result


def _latency_cpu_factor(expected_latency_seconds: float) -> float:
    if expected_latency_seconds <= 0.7:
        return 1.2
    if expected_latency_seconds < 1.0:
        return 1.1
    if expected_latency_seconds <= 5.0:
        return .7
    return .7


def _assess_jobmanager_size(input_params: EstimationInput) -> tuple[int, int]:
    if input_params.num_distinct_keys <= 10000000: # 10 million keys
        jm_tshirt_size ='S'
    elif input_params.num_distinct_keys <= 100000000: # 100 million keys
        jm_tshirt_size ='M'
    else:
        jm_tshirt_size ='L'
    return JM_TSHIRT_CPU_MB[jm_tshirt_size][0], JM_TSHIRT_CPU_MB[jm_tshirt_size][1]


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


def _throughput_cores(total_throughput_mb_per_sec: float, input_params: EstimationInput) -> float:
    """
    Cores to process the throughput, summed per statement type and scaled by the latency factor
    and number of applications.

    No per-statement core cap: a statement is parallelized across many TaskManagers/slots, so its
    CPU need scales with throughput. The real ceiling is distinct-key count / maxParallelism, not
    cores-per-TM; that bound is not modeled here.
    """
    simple_mbps = SIMPLE_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    medium_mbps = MEDIUM_RPS * input_params.avg_record_size_bytes / (1024 * 1024)
    complex_mbps = COMPLEX_RPS * input_params.avg_record_size_bytes / (1024 * 1024)

    simple_cores = (total_throughput_mb_per_sec / simple_mbps) * input_params.simple_statements
    medium_cores = (total_throughput_mb_per_sec / medium_mbps) * input_params.medium_statements
    complex_cores = (total_throughput_mb_per_sec / complex_mbps) * input_params.complex_statements

    cores = (
        (simple_cores + medium_cores + complex_cores)
        * _latency_cpu_factor(input_params.expected_latency_seconds)
        * max(1, input_params.number_flink_applications)
    )
    logger.info(
        "throughput cores simple=%.2f medium=%.2f complex=%.2f total=%.2f",
        simple_cores, medium_cores, complex_cores, cores,
    )
    return cores



def _defaulting_input_params(input_params: EstimationInput) -> EstimationInput:
    if input_params.worker_node_type == "VM":
        mem_mb, cpus, disk_gb = VM_TSHIRT_MB_CPU[input_params.worker_node_t_size]
        input_params.worker_node_memory_mb = mem_mb
        input_params.worker_node_cpu_max = cpus
        input_params.worker_node_disk_gb = disk_gb

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
