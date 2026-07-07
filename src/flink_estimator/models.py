"""
Pydantic models for Flink Resource Estimator.

This module contains all data models used for input validation,
estimation results, and file persistence.
"""

import math

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Literal, Optional

# A "CP Flink node" is a normalized buy unit: a collection of 8 cores running CP Flink workloads
# (TaskManagers + JobManagers). Lives here (not estimation.py) so persistence-format migration
# can derive cp_flink_nodes for legacy saved files.
CP_FLINK_NODE_CORES = 8

# When worker_node_type is VM, memory / CPU / local NVMe disk are derived from worker_node_t_size.
# Tuple order: (memory_mb, cpu_cores, local_disk_gb). Local disk is node-local NVMe for RocksDB state.
VM_TSHIRT_MB_CPU = {
    # memory_mb, cpu_cores, local_disk_gb
    "S": (16384, 8, 512),
    "M": (65536, 16, 2048),
    "L": (96448, 48, 6144),
}

class EstimationInput(BaseModel):
    """Input parameters for Flink estimation"""
    project_name: str = Field(..., min_length=1, max_length=100, description="Name of the project")
    messages_per_second: int = Field(default=5000, gt=0, description="Expected messages per second")
    avg_record_size_bytes: int = Field(default=512, gt=0, description="Average record size in bytes")
    number_flink_applications: int = Field(default=1, ge=1, description="Number of Flink applications")
    num_distinct_keys: int = Field(default=100_000, ge=1, description="Number of distinct keys for partitioning")
    data_skew_risk: Literal["low", "medium", "high"] = Field(default="low", description="Risk level of data skew")
    bandwidth_capacity_gbps: int = Field(
        default=10, gt=0, description="Network bandwidth capacity in Gbps (decimal gigabits per second)"
    )
    expected_latency_seconds: float = Field(default=5.0, gt=0, description="Expected end-to-end latency in seconds")
    simple_statements: int = Field(default=2, ge=0, description="Number of simple statements")
    medium_statements: int = Field(default=1, ge=0, description="Number of medium complexity statements")
    complex_statements: int = Field(default=1, ge=0, description="Number of complex statements")

    worker_node_memory_mb: float = Field(
        default=VM_TSHIRT_MB_CPU["S"][0],
        gt=0,
        le=512 * 1024,
        description="Target memory per worker node for sizing (MB); web form overwrites from GB; VM from T-shirt",
    )
    worker_node_cpu_max: int = Field(
        default=VM_TSHIRT_MB_CPU["S"][1],
        ge=2,
        le=256,
        description="Maximum CPU cores per worker node / TaskManager (instance shape limit)",
    )
    worker_node_disk_gb: int = Field(
        default=VM_TSHIRT_MB_CPU["S"][2],
        gt=0,
        description="Local NVMe disk per worker node (GB) for RocksDB state; VM type overwrites from T-shirt",
    )
    cores_per_tm: float = Field(
        default=1.0,
        gt=0,
        description="CPU cores allocated per TaskManager (TM shape; fractional allowed)",
    )
    mem_per_tm_mb: int = Field(
        default=4096,
        gt=0,
        description="Process memory per TaskManager (MB); should evenly divide usable node RAM to avoid stranding",
    )
    nb_worker_nodes: int = Field(
        default=1,  # Even if for HA we need 3, starts with 1 so we can use the tool to size dev environments
        ge=1,
        description="Number of worker nodes (floor for total_nodes in estimates)",
    )
    worker_node_type: Literal["bare_metal", "VM"] = Field(
        default="bare_metal",
        description="Whether workers are bare metal or VMs",
    )
    worker_node_t_size: Optional[Literal["S", "M", "L"]] = Field(
        default=None,
        description="VM T-shirt size (required when worker_node_type is VM)",
    )

    @field_validator("project_name")
    def validate_project_name(cls, v):
        if not v or v.isspace():
            raise ValueError("Project name cannot be empty or just whitespace")
        return v.strip()

    @model_validator(mode="after")
    def vm_tshirt_requires_size_and_sets_sku(self):
        if self.worker_node_type != "VM":
            return self
        if self.worker_node_t_size is None:
            raise ValueError("worker_node_t_size is required when worker_node_type is VM")
        mem_mb, cpus, disk_gb = VM_TSHIRT_MB_CPU[self.worker_node_t_size]
        self.worker_node_memory_mb = mem_mb
        self.worker_node_cpu_max = cpus
        self.worker_node_disk_gb = disk_gb
        return self

    @property
    def total_statements(self) -> int:
        return (self.simple_statements + self.medium_statements + self.complex_statements) * self.number_flink_applications

    @property
    def total_throughput_mb_per_sec(self) -> float:
        return (self.messages_per_second * self.avg_record_size_bytes) / (1024 * 1024)

    @property
    def state_size_bytes(self) -> int:
        """Total keyed state held across stateful (medium + complex) statements and applications.
        Under the RocksDB backend this is an on-disk (local NVMe) requirement, not RAM."""
        return (
            self.num_distinct_keys
            * (self.medium_statements + self.complex_statements)
            * self.avg_record_size_bytes
            * self.number_flink_applications
        )


class InputSummary(BaseModel):
    """Summary of input parameters with calculated values"""
    messages_per_second: int
    avg_record_size_bytes: int
    total_throughput_mb_per_sec: float
    num_distinct_keys: int
    data_skew_risk: str
    bandwidth_capacity_mbps: int
    expected_latency_seconds: float
    simple_statements: int
    medium_statements: int
    complex_statements: int
    total_statements: int
    worker_node_memory_capacity_mb: float
    worker_node_cpu_capacity: int
    nb_worker_nodes: int
    worker_node_t_size: Optional[Literal["S", "M", "L"]]


class ResourceEstimates(BaseModel):
    """Estimated resource requirements.

    Primary buy unit is ``cp_flink_nodes`` (8 cores each, derived from aggregate CPUs of all
    CP Flink workloads — TaskManagers + JobManagers). ``total_worker_node_needed`` is a secondary,
    VM-shape-dependent bin-packing metric bounded by CPU, RAM, and local disk, so the
    recommended fleet always carries at least ``total_cpus`` cores.
    """
    cp_flink_nodes: int = Field(..., description="PRIMARY: ⌈total CPUs / 8⌉; a CP Flink node is 8 cores")
    total_cpus: int = Field(..., description="Aggregate CPU cores the workload needs (TaskManagers + JobManagers)")
    provisioned_cores: int = Field(
        ...,
        description="Cores physically provisioned = worker nodes × cores per node. Never below "
        "total_cpus; exceeds it when a RAM/disk-constrained node shape forces more nodes than "
        "the compute need.",
    )
    total_memory_mb: int = Field(
        ...,
        description="Aggregate Flink process RAM (TaskManagers + JobManagers). Keyed state is NOT "
        "included — under RocksDB it lives on local disk (total_disk_gb).",
    )
    total_disk_gb: int = Field(..., description="Local NVMe disk for RocksDB state incl. compaction headroom")
    total_worker_node_needed: int = Field(..., description="SECONDARY: VM nodes to bin-pack CPU, RAM, and disk")
    processing_load_score: float


class JobManagerConfig(BaseModel):
    """JobManager configuration specifications"""
    count: int
    memory_mb: int
    total_cpus: float = Field(..., ge=0.5, description="CPU cores (Kubernetes cpu units; fractional allowed)")


class TaskManagerConfig(BaseModel):
    """TaskManager configuration specifications"""
    count: int
    total_memory_mb: int
    total_cpus: int
    memory_mb_each: float


class ClusterRecommendations(BaseModel):
    """Cluster configuration recommendations"""
    jobmanager: JobManagerConfig
    taskmanagers: TaskManagerConfig

class CapacityAnalysis(BaseModel):
    """Capacity analysis"""
    total_flink_statements: int
    total_flink_applications: int


class SizingDiagnostics(BaseModel):
    """Intermediate sizing values and which resource bounds the secondary worker-node count.

    CPU drives the primary CP Flink node count; CPU, RAM, and local disk each independently
    floor the secondary VM/worker-node bin-packing. ``worker_node_bounding_factor`` says which
    of them forced the worker-node count.
    """
    tm_cores: float = Field(..., description="Cores for TaskManager throughput work (uncapped, fractional)")
    jm_cores: float = Field(..., description="Cores for JobManagers (jm_cpu × applications)")
    nb_taskmanagers: int = Field(
        ...,
        description="TaskManager count: ⌈tm_cores / cores_per_tm⌉, raised to the disk-node count "
        "(RocksDB state is node-local) and grown further when per-TM buffer memory would "
        "exceed the per-worker cap",
    )
    mem_per_tm_mb: int = Field(..., description="Process memory per TaskManager (MB)")
    ram_total_mb: int = Field(..., description="Aggregate TaskManager process memory incl. buffer headroom (MB)")
    state_size_gb: float = Field(..., description="Total keyed state size (GB) before RocksDB headroom")
    required_disk_gb: int = Field(..., description="state_size × amplification (GB) bin-packed onto local NVMe")
    tms_per_node: int = Field(..., description="TaskManagers that fit per worker node by usable RAM")
    stranded_ram_mb_per_node: int = Field(..., description="Usable RAM left over per node when mem_per_tm does not evenly divide it")
    cpu_nodes: int = Field(..., description="Worker nodes required to physically carry total cores (⌈total_cores / cores_per_node⌉)")
    ram_nodes: int = Field(..., description="Worker nodes required to hold aggregate RAM")
    disk_nodes: int = Field(..., description="Worker nodes required to hold required_disk_gb")
    worker_node_bounding_factor: Literal["cpu", "ram", "disk", "balanced"] = Field(
        ..., description="Whether CPU, RAM, or local disk drives the worker-node count"
    )


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
    scaling_recommendations: Optional[ScalingRecommendations] = None
    capacity_analysis: CapacityAnalysis
    sizing_diagnostics: Optional[SizingDiagnostics] = None

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

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_saved_format(cls, data):
        """Files saved before the sizing-model redesign lack cp_flink_nodes /
        provisioned_cores / total_disk_gb and carry an incompatible sizing_diagnostics
        shape. Derive what the legacy fields allow and drop the incompatible diagnostics
        so old files keep reloading."""
        if not isinstance(data, dict):
            return data
        results = data.get("estimation_results")
        if not isinstance(results, dict):
            return data
        estimates = results.get("resource_estimates")
        if not isinstance(estimates, dict) or "cp_flink_nodes" in estimates:
            return data
        total_cpus = int(estimates.get("total_cpus") or 0)
        worker_nodes = int(estimates.get("total_worker_node_needed") or 0)
        inputs = data.get("input_parameters")
        cores_per_node = int(inputs.get("worker_node_cpu_max") or 0) if isinstance(inputs, dict) else 0
        # Rewrite copies, not the caller's dicts.
        estimates = {
            **estimates,
            "cp_flink_nodes": max(1, math.ceil(total_cpus / CP_FLINK_NODE_CORES)),
            "provisioned_cores": worker_nodes * cores_per_node,
            "total_disk_gb": 0,
        }
        results = {**results, "resource_estimates": estimates}
        diagnostics = results.get("sizing_diagnostics")
        if isinstance(diagnostics, dict) and "tm_cores" not in diagnostics:
            results["sizing_diagnostics"] = None
        return {**data, "estimation_results": results}
