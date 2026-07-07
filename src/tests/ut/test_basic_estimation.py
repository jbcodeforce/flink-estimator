"""
Unit tests: internal estimation helpers and VM-integrated E2E regressions.

Covers _defaulting_input_params; _assess_jobmanager_size; _throughput_cores (uncapped CPU);
_state_disk_gb; _pack_worker_nodes; _resolve_per_tm_memory_mb; _network_buffer_min_process_memory_mb;
_latency_cpu_factor; and full calculate_flink_estimation with VM t-shirts or custom worker fields.

Sizing model (see docs/superpowers/specs/2026-06-23-flink-estimator-sizing-model-redesign-design.md):
  PRIMARY   cp_flink_nodes = ceil(total_cores / 8); total_cores = throughput cores + JM cores.
  SECONDARY worker/VM nodes = bin-pack TM RAM and RocksDB local disk; CPU never bounds node count.
"""

import pytest
import math
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import (
    calculate_flink_estimation,
    _defaulting_input_params,
    _assess_jobmanager_size,
    _throughput_cores,
    _state_disk_gb,
    _pack_worker_nodes,
    _resolve_per_tm_memory_mb,
    _network_buffer_min_process_memory_mb,
    _latency_cpu_factor,
    CP_FLINK_NODE_CORES,
    STATE_DISK_AMPLIFICATION,
    TM_MEM_MB,
)
import os

os.environ.pop("FLINK_ESTIMATOR_DEBUG", None)


@pytest.fixture
def vm_s_estimation_input() -> EstimationInput:
    """Shared EstimationInput: VM T-shirt S (project name + SKU). Inject into tests; use model_copy(update=...) to specialize."""
    return EstimationInput(
        project_name="VM Test",
        worker_node_type="VM",
        worker_node_t_size="S",
    )


class TestPrivateHelpers:
    """Private helpers: _defaulting_input_params, _assess_jobmanager_size, _throughput_cores, packing, disk."""

    def test_vm_default_parameters(self, vm_s_estimation_input):
        """VM S/M/L via _defaulting_input_params: memory, CPU, and local disk from t-shirt."""
        input_params = _defaulting_input_params(vm_s_estimation_input)
        assert input_params.worker_node_memory_mb == 16384
        assert input_params.worker_node_cpu_max == 8
        assert input_params.worker_node_disk_gb == 512
        assert input_params.nb_worker_nodes == 1
        # Statement counts come from EstimationInput model defaults, not from _defaulting_input_params
        assert input_params.simple_statements == 2
        assert input_params.medium_statements == 1
        assert input_params.complex_statements == 1
        assert input_params.number_flink_applications == 1

        input_params = _defaulting_input_params(
            EstimationInput(project_name="VM Test", worker_node_type="VM", worker_node_t_size="M")
        )
        assert input_params.worker_node_memory_mb == 65536
        assert input_params.worker_node_cpu_max == 16
        assert input_params.worker_node_disk_gb == 2048

        input_params = _defaulting_input_params(
            EstimationInput(project_name="VM Test", worker_node_type="VM", worker_node_t_size="L")
        )
        assert input_params.worker_node_memory_mb == 96448
        assert input_params.worker_node_cpu_max == 48
        assert input_params.worker_node_disk_gb == 6144

        input_params = _defaulting_input_params(
            EstimationInput(project_name="Bare Metal Test", worker_node_type="bare_metal", worker_node_t_size="S")
        )
        assert input_params.worker_node_memory_mb == 16384
        assert input_params.worker_node_cpu_max == 8
        assert input_params.worker_node_disk_gb == 512
   

    def test_latency_cpu_factor(self):
        """_latency_cpu_factor: multipliers at 0.5, 1.0, 3.0, 5.0, 10.0 s expected latency."""
        assert _latency_cpu_factor(0.5) == 1.2
        assert _latency_cpu_factor(1.0) == 0.7
        assert _latency_cpu_factor(3.0) == .7       # 3.0s latency is the cutoff for the 0.7 multiplier
        assert _latency_cpu_factor(5.0) == .7
        assert _latency_cpu_factor(10.0) == .7

    def test_jobmanager_size(self, vm_s_estimation_input):
        """_assess_jobmanager_size: JM vCPU and memory scale with num_distinct_keys tiers (10M, 100M, 1B+)."""
        input_params = _defaulting_input_params(vm_s_estimation_input)
        input_params.num_distinct_keys = 10_000_000
        assert _assess_jobmanager_size(input_params) == (1, 2048)
        input_params.num_distinct_keys = 100_000_000
        assert _assess_jobmanager_size(input_params) == (2, 4096)
        input_params.num_distinct_keys = 1_000_000_000
        assert _assess_jobmanager_size(input_params) == (4, 8192)

    def test_throughput_cores_uncapped(self, vm_s_estimation_input):
        """
        _throughput_cores: CPU scales with throughput(no per-statement cap). 
        A complex op that needs 60 cores to keep up is counted as 60, not clamped.
        """
        input_params = _defaulting_input_params(
            vm_s_estimation_input.model_copy(
                update={
                    "simple_statements": 1,
                    "medium_statements": 0,
                    "complex_statements": 0,
                    "avg_record_size_bytes": 1024,
                    "expected_latency_seconds": 1.0,  # latency factor 1.0
                    "messages_per_second": 150_000,
                }
            )
        )
        thr = input_params.total_throughput_mb_per_sec  # 146MB/s 296 MB/s
        cores = _throughput_cores(thr, input_params)
        assert cores >= 4
        assert cores <= 6
        input_params.medium_statements = 1
        cores = _throughput_cores(thr, input_params)
        print(f"cores: {cores}")
        assert cores >= 13
        input_params.complex_statements = 1
        cores = _throughput_cores(thr, input_params)
        print(f"cores: {cores}")
        assert cores <= 35

    def test_throughput_cores_scales_with_statements_and_apps(self, vm_s_estimation_input):
        """Doubling statements (and applications) scales throughput cores proportionally."""
        base = _defaulting_input_params(
            vm_s_estimation_input.model_copy(
                update={"simple_statements": 1, "medium_statements": 1, "complex_statements": 1,
                        "expected_latency_seconds": 5.0, "messages_per_second": 100000, "number_flink_applications": 1}
            )
        )
        thr = base.total_throughput_mb_per_sec
        one = _throughput_cores(thr, base)
        two_apps = base.model_copy(update={"number_flink_applications": 2})
        assert _throughput_cores(thr, two_apps) == pytest.approx(2 * one, rel=1e-6)

    def test_state_disk_gb(self):
        """_state_disk_gb: state size = keys × stateful stmts × record × apps, ×1.5 RocksDB headroom."""
        input_params = EstimationInput(
            project_name="State disk",
            num_distinct_keys=50_000_000,
            avg_record_size_bytes=2000,
            medium_statements=2,
            complex_statements=3,
            number_flink_applications=1,
        )
        state_gb, required_gb = _state_disk_gb(input_params)
        expected_state = (50_000_000 * 5 * 2000 * 1) / (1024 ** 3)
        assert state_gb == pytest.approx(expected_state, rel=1e-6)  # ~465.66 GiB
        assert required_gb == math.ceil(expected_state * STATE_DISK_AMPLIFICATION)  # 699
        # Stateless statements (simple only) hold no keyed state.
        stateless = input_params.model_copy(update={"medium_statements": 0, "complex_statements": 0})
        assert _state_disk_gb(stateless) == (0.0, 0)

    def test_network_buffer_min_process_memory_mb(self):
        """_network_buffer_min_process_memory_mb: tight latency yields material per-TM buffer memory."""
        input_params = EstimationInput(
            project_name="Network Buffer Test",
            avg_record_size_bytes=512,
            medium_statements=1,
            complex_statements=1,
            simple_statements=1,
            expected_latency_seconds=0.5,
            messages_per_second=1000,
        )
        tmbps = input_params.total_throughput_mb_per_sec
        assert _network_buffer_min_process_memory_mb(input_params, tmbps, 1) >= 200  # MB

    def test_pack_worker_nodes_ram_and_disk(self, vm_s_estimation_input):
        """_pack_worker_nodes: node count is max(RAM pack, disk pack); reports stranding and bounding factor."""
        input_params = _defaulting_input_params(vm_s_estimation_input)  # S: 16 GB / 512 GB disk
        # 5 TMs of 4 GB: usable 15872 MB -> 3 TMs/node, 3584 MB stranded -> 2 nodes for TMs.
        pack = _pack_worker_nodes(input_params, nb_taskmanagers=5, per_tm_mem_mb=4096,
                                  jm_memory=2048, required_disk_gb=100)
        assert pack["tms_per_node"] == 3
        assert pack["stranded_ram_mb_per_node"] == 15872 - 3 * 4096
        assert pack["ram_nodes"] == 2
        assert pack["disk_nodes"] == 1  # 100 GB fits one 512 GB node
        assert pack["worker_nodes"] == 2
        assert pack["bounding"] == "ram"
        # Disk-bound: huge state forces more nodes than RAM.
        pack2 = _pack_worker_nodes(input_params, nb_taskmanagers=1, per_tm_mem_mb=4096,
                                   jm_memory=2048, required_disk_gb=5000)
        assert pack2["disk_nodes"] == math.ceil(5000 / 512)
        assert pack2["worker_nodes"] == pack2["disk_nodes"]
        assert pack2["bounding"] == "disk"

    def test_resolve_per_tm_memory_floor_and_buffer(self, vm_s_estimation_input):
        """_resolve_per_tm_memory_mb: never below configured mem_per_tm; rises with tight-latency buffers."""
        relaxed = _defaulting_input_params(
            vm_s_estimation_input.model_copy(update={"expected_latency_seconds": 10.0, "messages_per_second": 1000})
        )
        assert _resolve_per_tm_memory_mb(relaxed, relaxed.total_throughput_mb_per_sec, 1) == TM_MEM_MB


class TestBasicEstimation:
    """End-to-end calculate_flink_estimation: primary CP Flink node count and secondary VM packing."""

    def _assert_consistent(self, result):
        """Invariants that must hold for every estimate."""
        re = result.resource_estimates
        assert re.cp_flink_nodes == max(1, math.ceil(re.total_cpus / CP_FLINK_NODE_CORES))
        assert re.total_cpus >= 1
        assert re.total_worker_node_needed >= 1
        # TM cores + aggregate JM cores (per-JM × count) reconcile with the reported total.
        cc = result.cluster_recommendations
        jm_aggregate = cc.jobmanager.count * math.ceil(cc.jobmanager.total_cpus)
        assert re.total_cpus == cc.taskmanagers.total_cpus + jm_aggregate
        # Provisioned cores = worker nodes × cores/node; never below the compute need.
        assert re.provisioned_cores == re.total_worker_node_needed * result.input_summary.worker_node_cpu_capacity
        assert re.provisioned_cores >= re.total_cpus

    def test_default_settings_estimation(self, vm_s_estimation_input):
        """VM S defaults: low throughput, tiny state. 1 CP Flink node, packs onto a single VM."""
        result = calculate_flink_estimation(vm_s_estimation_input)
        self._assert_consistent(result)
        re = result.resource_estimates
        # throughput ~2.87 cores + 1 JM core -> 4 total -> 1 CP Flink node
        assert re.cp_flink_nodes == 1
        assert re.total_cpus == 3
        assert re.total_worker_node_needed == 1
        assert re.total_disk_gb >= 1  # 100k keys × 2 stateful × 512 B ~ 0.1 GiB -> 1 GB
        tm = result.cluster_recommendations.taskmanagers
        assert tm.memory_mb_each == 4096
        assert tm.count == math.ceil(result.sizing_diagnostics.tm_cores)

    def test_minimal_workload(self, vm_s_estimation_input):
        """VM S: 5000 msg/s, one stateless simple op, 10M keys -> tiny footprint, single node."""
        input_params = vm_s_estimation_input.model_copy(
            update={
                "project_name": "Minimal Test",
                "messages_per_second": 5000,
                "avg_record_size_bytes": 512,
                "num_distinct_keys": 10_000_000,
                "expected_latency_seconds": 5.0,
                "simple_statements": 1,
                "medium_statements": 0,
                "complex_statements": 0,
            }
        )
        result = calculate_flink_estimation(input_params)
        self._assert_consistent(result)
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(2.44, rel=1e-1)
        assert result.input_summary.worker_node_cpu_capacity == 8
        # Stateless: no keyed state -> no disk.
        assert result.resource_estimates.total_disk_gb == 0
        assert result.resource_estimates.cp_flink_nodes == 1
        assert result.resource_estimates.total_worker_node_needed == 1
        assert result.cluster_recommendations.jobmanager.total_cpus == 1

    def test_complex_throughput_drives_cores_not_state(self):
        """The headline regression: 150k msg/s, 1+2+3 stmts, 50M keys, 60s latency on VM M.
        CPU is throughput-driven (~216 cores -> 27 CP Flink nodes); state is ~700 GB of disk that
        fits the VM-node memory packing. No state-inflated CPU."""
        input_params = EstimationInput(
            project_name="EOD payment reconciliation",
            messages_per_second=150000,
            avg_record_size_bytes=2000,
            number_flink_applications=1,
            simple_statements=1,
            medium_statements=2,
            complex_statements=3,
            num_distinct_keys=50_000_000,
            data_skew_risk="medium",
            expected_latency_seconds=60,
            bandwidth_capacity_gbps=10,
            worker_node_type="VM",
            worker_node_t_size="M",
        )
        result = calculate_flink_estimation(input_params)
        self._assert_consistent(result)
        re = result.resource_estimates
        diag = result.sizing_diagnostics
        assert diag.tm_cores == pytest.approx(80.74, rel=1e-2)
        assert re.total_cpus == 83  # 214 TM + 2 JM
        assert re.cp_flink_nodes == 11  # ceil(216 / 8)
        assert re.total_disk_gb == 699  # 465.66 GiB × 1.5
        assert diag.worker_node_bounding_factor == "ram"
        # The old model reported 1168 CPUs against 20 nodes; the new model never inflates CPU by state.
        assert re.total_cpus < 300

    def test_cp_flink_nodes_invariant_provisioned_cores_shape_dependent(self):
        """cp_flink_nodes (compute demand) is the same on S and M; provisioned_cores reveals that a
        RAM-light S fleet drags along far more cores than the workload needs."""
        base = dict(
            project_name="Shape compare", messages_per_second=150000, avg_record_size_bytes=2000,
            simple_statements=1, medium_statements=2, complex_statements=3,
            num_distinct_keys=50_000_000, expected_latency_seconds=60, number_flink_applications=1,
        )
        s = calculate_flink_estimation(EstimationInput(**base, worker_node_type="VM", worker_node_t_size="S"))
        m = calculate_flink_estimation(EstimationInput(**base, worker_node_type="VM", worker_node_t_size="M"))
        self._assert_consistent(s)
        self._assert_consistent(m)
        # Compute demand is shape-independent.
        assert s.resource_estimates.cp_flink_nodes == m.resource_estimates.cp_flink_nodes
        assert s.resource_estimates.total_cpus == m.resource_estimates.total_cpus
        # RAM-light S forces more nodes -> more provisioned cores than the richer M shape.
        assert s.resource_estimates.provisioned_cores > m.resource_estimates.provisioned_cores
        assert s.resource_estimates.provisioned_cores > s.resource_estimates.total_cpus

    def test_high_application_count_folds_jm_into_cores(self):
        """Many applications: each app's JobManager cores count toward the CP Flink node total."""
        input_params = EstimationInput(
            project_name="Many apps",
            messages_per_second=50000,
            avg_record_size_bytes=2048,
            num_distinct_keys=10_000_000,
            simple_statements=5,
            medium_statements=3,
            complex_statements=2,
            number_flink_applications=10,
            worker_node_type="VM",
            worker_node_t_size="M",
        )
        result = calculate_flink_estimation(input_params)
        self._assert_consistent(result)
        # 10 apps × JM(S tier =1 core) = 10 JM cores folded in.
        assert result.sizing_diagnostics.jm_cores == 10
        assert result.cluster_recommendations.jobmanager.count == 10

    def test_disk_bound_large_state(self):
        """Huge state, modest throughput: worker-node count is driven by local disk, not RAM."""
        input_params = EstimationInput(
            project_name="Disk bound",
            messages_per_second=2000,
            avg_record_size_bytes=4096,
            num_distinct_keys=200_000_000,
            simple_statements=0,
            medium_statements=2,
            complex_statements=2,
            number_flink_applications=1,
            worker_node_type="VM",
            worker_node_t_size="M",
        )
        result = calculate_flink_estimation(input_params)
        self._assert_consistent(result)
        diag = result.sizing_diagnostics
        assert diag.required_disk_gb > 0
        assert diag.disk_nodes > diag.ram_nodes
        assert diag.worker_node_bounding_factor == "disk"
        assert result.resource_estimates.total_worker_node_needed == diag.disk_nodes


if __name__ == "__main__":
    pytest.main()
