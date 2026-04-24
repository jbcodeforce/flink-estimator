"""
Unit tests: internal estimation helpers and VM-integrated E2E regressions.

Covers _defaulting_input_params; _assess_jobmanager_size, _assess_free_mem_per_node,
_assess_taskmanager_based_on_state, _assess_taskmanager_based_on_throughput; _greedy_pack_taskmanagers;
_latency_cpu_factor; and full calculate_flink_estimation with VM t-shirts or custom worker fields.
"""

import pytest
import math
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import(
    calculate_flink_estimation,
     _defaulting_input_params, 
     _assess_taskmanager_based_on_state,
     _assess_jobmanager_size,
     _assess_free_mem_per_node,
     _greedy_pack_taskmanagers,
     _assess_taskmanager_based_on_throughput,
     _latency_cpu_factor,
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
    """Private helpers: _defaulting_input_params, _assess_*, _greedy_pack_taskmanagers, _latency_cpu_factor."""

    def test_vm_default_parameters(self, vm_s_estimation_input):
        """VM S/M/L via _defaulting_input_params: memory and CPU from t-shirt; other fields stay model defaults."""
        input_params = _defaulting_input_params(vm_s_estimation_input)
        assert input_params.worker_node_memory_mb == 16384
        assert input_params.worker_node_cpu_max == 8
        assert input_params.nb_worker_nodes == 1
        # Statement counts come from EstimationInput model defaults, not from _defaulting_input_params
        assert input_params.simple_statements == 2
        assert input_params.medium_statements == 1
        assert input_params.complex_statements == 1
        assert input_params.data_skew_risk == "low"
        assert input_params.bandwidth_capacity_gbps == 10
        assert input_params.expected_latency_seconds == 5.0

        # Medium VM
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="M"
        )
        input_params = _defaulting_input_params(input_params)
        assert input_params.worker_node_memory_mb == 65536
        assert input_params.worker_node_cpu_max == 16
        # Large VM
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="L"
        )
        input_params = _defaulting_input_params(input_params)
        print(input_params.model_dump_json(indent=2))
        assert input_params.worker_node_memory_mb == 96448
        assert input_params.worker_node_cpu_max == 48


    def test_latency_cpu_factor(self):
        """_latency_cpu_factor: multipliers at 0.5, 1.0, 3.0, 5.0, 10.0 s expected latency."""
        assert _latency_cpu_factor(0.5) == 1.5
        assert _latency_cpu_factor(1.0) == 1.2
        assert _latency_cpu_factor(3.0) == 1.1
        assert _latency_cpu_factor(5.0) == 1.0
        assert _latency_cpu_factor(10.0) == 1.0

    def test_jobmanager_size(self, vm_s_estimation_input):
        """_assess_jobmanager_size: JM vCPU and memory scale with num_distinct_keys tiers (10M, 100M, 1B+)."""
        input_params = _defaulting_input_params(vm_s_estimation_input)
        input_params.num_distinct_keys=10_000_000
        input_params.avg_record_size_bytes=512
        input_params.complex_statements=1
        input_params.medium_statements=1
        input_params.simple_statements=1
        jm_cpu,jm_memory= _assess_jobmanager_size(input_params)
        print(f"jm_cpu: {jm_cpu}, jm_memory: {jm_memory}")
        assert jm_cpu == 1
        assert jm_memory == 2048
        input_params.num_distinct_keys=100_000_000
        jm_cpu,jm_memory= _assess_jobmanager_size(input_params)
        print(f"jm_cpu: {jm_cpu}, jm_memory: {jm_memory}")
        assert jm_cpu == 2
        assert jm_memory == 4096
        input_params.num_distinct_keys=1000000000
        jm_cpu,jm_memory= _assess_jobmanager_size(input_params)
        print(f"jm_cpu: {jm_cpu}, jm_memory: {jm_memory}")
        assert jm_cpu == 4
        assert jm_memory == 8192

    def test_free_mem_per_node(self, vm_s_estimation_input):
        """_assess_free_mem_per_node: per-node free MB after OS reserve; node 0 deducts JM for all apps."""
        input_params = _defaulting_input_params(
            vm_s_estimation_input.model_copy(update={"nb_worker_nodes": 3})
        )
        free_mem_per_node, total_free_mem = _assess_free_mem_per_node(input_params, 2048)
        print(f"free_mem_per_node: {free_mem_per_node}, {total_free_mem}")
        # first node has one job manager.
        assert free_mem_per_node == [13824,15872,15872]
        assert total_free_mem >= 45000
        input_params.nb_worker_nodes = 1
        free_mem_per_node, total_free_mem = _assess_free_mem_per_node(input_params, 2048)
        print(f"free_mem_per_node: {free_mem_per_node}, {total_free_mem}")
        assert free_mem_per_node == [13824]
        assert total_free_mem >= 13824

    def test_nb_taskmanagers_from_state_size_small_vm(self, vm_s_estimation_input):
        """_assess_taskmanager_based_on_state: at least one TM; larger state (more keys/bytes) increases TM count and node split."""
        # Small VM with small state too.
        input_params = vm_s_estimation_input.model_copy(
            update={
                "nb_worker_nodes": 1,
                "simple_statements": 1,
                "num_distinct_keys": 1_000_000,  # 1 million keys
                "avg_record_size_bytes": 512,
                "complex_statements": 0,
                "medium_statements": 1,  # need at least one medium or complex statement to have state
            }
        )
        jm_memory = 2048
        total_memory_mb, nb_taskmanagers, node_allocations = _assess_taskmanager_based_on_state(input_params, jm_memory)
        print(f"nb_taskmanagers: {total_memory_mb} {nb_taskmanagers}, {node_allocations}")
        assert nb_taskmanagers == 1
        assert node_allocations == [1]

        # increase the state size to have more TMs
        print("\n --- increase the state size to 20 million keys at 1k value size to have more TM\n")
        input_params.num_distinct_keys=20_000_000 # 20 million keys
        input_params.avg_record_size_bytes=1024
        total_memory_mb, nb_taskmanagers, node_allocations = _assess_taskmanager_based_on_state(input_params, jm_memory)
        print(f"nb_taskmanagers: {total_memory_mb} {nb_taskmanagers}, {node_allocations}")
        assert nb_taskmanagers == 12
        assert node_allocations == [3,3,3,3]


    def test_nb_taskmanagers_from_state_size_large_vm(self):
        """
        Large VM, big state due to millions of keys and more flink stateful operators
        """
        # Large VM, state bigger than TM heap size but lot of capacity per host
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="L",
            nb_worker_nodes=1,
            simple_statements=1,
            num_distinct_keys=20000000, # 20 million keys,
            avg_record_size_bytes=1024,
            complex_statements=1,
            medium_statements=4,
            number_flink_applications=1,
            expected_latency_seconds=5.0,
        )
        jm_memory = 4096
        total_memory_mb, nb_taskmanagers, node_allocations = _assess_taskmanager_based_on_state(input_params, jm_memory)
        print(f"nb_taskmanagers: {total_memory_mb} {nb_taskmanagers}, {node_allocations}")
        # Large state + 4 medium + 1 complex yields 60 TMs; placement may be [18,21,21] or [20,20,20]
        assert nb_taskmanagers == 60
        assert node_allocations in [[18,21,21],[20,20,20]]
 

    def test_greedy_pack_taskmanagers_invariants(self):
        """Greedy pack: one count per node, total TMs placed, per-node headroom."""
        # Same shape as S VM: JM on node 0.
        free_mem = [13824, 15872, 15872]
        nb_tm = 2
        alloc, mx, ok, remaining = _greedy_pack_taskmanagers(free_mem, nb_tm, TM_MEM_MB)
        print(f"out_of_mem: {alloc}, {mx}, {ok}, {remaining}")
        assert ok
        assert len(alloc) == 3
        assert sum(alloc) == nb_tm
        assert mx == 1
        for i, c in enumerate(alloc):
            assert c * TM_MEM_MB <= free_mem[i]
        out_of_mem, _mx, nok, remaining = _greedy_pack_taskmanagers(
            [100, 100, 100], 1, TM_MEM_MB
        )
        
        assert nok is False
        assert sum(out_of_mem) < 1


    def test_cpu_need_for_throughput(self, vm_s_estimation_input):
        """_assess_taskmanager_based_on_throughput: fixed throughput; then add a medium op; then widen VM to M (more cores per node)."""
        input_params = _defaulting_input_params(
            vm_s_estimation_input.model_copy(
                update={
                    "simple_statements": 1,
                    "medium_statements": 0,
                    "complex_statements": 0,
                    "avg_record_size_bytes": 1024,
                    "num_distinct_keys": 1000,
                    "data_skew_risk": "low",
                    "bandwidth_capacity_gbps": 10,  # 10 Gbps
                    "expected_latency_seconds": 5.0,
                    "number_flink_applications": 1,
                    "messages_per_second": 500_000,
                }
            )
        )
        total_throughput_mb_per_sec = input_params.messages_per_second * input_params.avg_record_size_bytes / (1024 * 1024)
        nb_taskmanagers, cpu_total, nb_worker_nodes = _assess_taskmanager_based_on_throughput(total_throughput_mb_per_sec, input_params, 1)
        print(f"\ntest_cpu_need_for_throughput: {total_throughput_mb_per_sec} MBps is cpu:{cpu_total} wn:{nb_worker_nodes} tm:{nb_taskmanagers}")
        assert cpu_total >= 5
        assert nb_worker_nodes == 1
        assert nb_taskmanagers == 1
        input_params.medium_statements = 1
        nb_taskmanagers, cpu_total, nb_worker_nodes = _assess_taskmanager_based_on_throughput(total_throughput_mb_per_sec, input_params, 1)
        print(f"test_cpu_need_for_throughput: {total_throughput_mb_per_sec} MBps is cpu:{cpu_total} wn:{nb_worker_nodes} tm:{nb_taskmanagers}")
        assert cpu_total >= 9
        assert nb_worker_nodes == 2
        assert nb_taskmanagers == 2
        input_params.worker_node_t_size = 'M'
        nb_taskmanagers, cpu_total, nb_worker_nodes = _assess_taskmanager_based_on_throughput(total_throughput_mb_per_sec, input_params, 1)
        print(f"test_cpu_need_for_throughput: {total_throughput_mb_per_sec} MBps is cpu:{cpu_total} wn:{nb_worker_nodes} tm:{nb_taskmanagers}")
        assert cpu_total >= 9
        assert nb_worker_nodes == 1
        assert nb_taskmanagers == 2


class TestBasicEstimation:
    """End-to-end calculate_flink_estimation: VM-integrated and custom worker shapes (memory/CPU) as noted per test."""

    def test_default_settings_estimation(self, vm_s_estimation_input):
        """VM S with fixture-only input: single TM+JM, totals and per-component numbers."""
        result = calculate_flink_estimation(vm_s_estimation_input)
        print(result.model_dump_json(indent=2))
        assert result.cluster_recommendations.taskmanagers.count == 1
        assert result.cluster_recommendations.taskmanagers.total_memory_mb == 4096
        assert result.cluster_recommendations.taskmanagers.total_cpus == 3
        assert result.cluster_recommendations.taskmanagers.memory_mb_each == 4096
        assert result.cluster_recommendations.jobmanager.count == 1
        assert result.resource_estimates.total_memory_mb == 6144
        assert result.resource_estimates.total_cpus == 4
        assert result.resource_estimates.total_worker_node_needed == 1
       
    def test_minimal_workload(self, vm_s_estimation_input):
        """VM S: 5000 msg/s, 512-byte records, one simple op; 10M keys, stateless medium/complex (0+0)."""
        input_params = vm_s_estimation_input.model_copy(
            update={
                "project_name": "Minimal Test",
                "messages_per_second": 5000,
                "avg_record_size_bytes": 512,
                "num_distinct_keys": 10_000_000,  # 10 million keys
                "data_skew_risk": "low",
                "bandwidth_capacity_gbps": 10,  # 10 Gbps
                "expected_latency_seconds": 5.0,
                "simple_statements": 1,  # stateless filtering
                "medium_statements": 0,  # deduplication, group by aggregation
                "complex_statements": 0,  # full left join
            }
        )
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify basic structure
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(2.44, rel=1e-1)
        assert result.input_summary.worker_node_memory_capacity_mb == pytest.approx(16384, rel=1e-2)
        assert result.input_summary.worker_node_cpu_capacity == 8

        # Check resource estimates are reasonable for minimal load
        assert result.resource_estimates.total_memory_mb >= 6144  # JM + TM aggregate
        assert result.resource_estimates.total_cpus >= 4  # JM + workload CPUs
        assert result.resource_estimates.total_worker_node_needed == 1

        # One TM fits this throughput on VM S; TM CPU bundle matches workload ceiling
        assert result.cluster_recommendations.taskmanagers.count == 1
        assert result.cluster_recommendations.taskmanagers.total_cpus == 3
        assert result.cluster_recommendations.jobmanager.total_cpus == 1

    def test_simple_workload(self):
        """VM L: 5000 msg/s at 512 bytes, 1 simple + 2 medium + 2 complex (5 statements), 1.0s latency (parallelism cap)."""
        # VM L has enough per-node memory to place many 4096 MB TMs when state requires it
        input_params = EstimationInput(
            project_name="Simple_Workload Test",
            messages_per_second=10000,
            avg_record_size_bytes=1024,
            num_distinct_keys=100_000, # 
            data_skew_risk="low",
            bandwidth_capacity_gbps= 10,  # very high Gbps value (estimator field; not the limiter here)
            expected_latency_seconds=5.0,
            worker_node_type="VM",
            worker_node_t_size="S",
            simple_statements=1,   # stateless filtering
            medium_statements=1,   # deduplication, group by aggregation
            complex_statements=1,   # full left join
        )
        input_params = _defaulting_input_params(input_params)
        
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify basic structure
        assert result.input_summary.total_statements == 3
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(9.77, rel=1e-1)
        
        # Check resource estimates are reasonable for a multi-statement workload
        assert result.resource_estimates.total_memory_mb >= 50800
        assert result.resource_estimates.total_cpus >= 45
        # NOT USED YETassert result.resource_estimates.processing_load_score == pytest.approx(0.35, rel=1e-1)
        
        # TM count follows memory and CPU; multiple TMs are valid when total_cpu is split across TMs
        assert result.cluster_recommendations.taskmanagers.count >= 12
        assert result.cluster_recommendations.jobmanager.total_cpus == pytest.approx(1, rel=1e-1)
        assert result.cluster_recommendations.taskmanagers.total_cpus == pytest.approx(47, rel=1e-1)

    
    def test_medium_work_nodes_with_simple_app(self):
        """VM M: 10k msg/s, 1k records, 10M keys, 1+1+1 statements; state drives TM count and totals."""
        input_params = EstimationInput(
            project_name="Medium Work Nodes Test",
            messages_per_second=10000,
            avg_record_size_bytes=1024,
            num_distinct_keys=10_000_000,
            simple_statements=1,
            medium_statements=1,
            complex_statements=1,
            number_flink_applications=1,
            expected_latency_seconds=5.0,
            worker_node_type="VM",
            worker_node_t_size="M",
            bandwidth_capacity_gbps= 10,  # 10 Gbps
        )
        input_params = _defaulting_input_params(input_params)
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify basic structure
        assert result.input_summary.total_statements == 3
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(9.77, rel=1e-1)
        # State (10M keys × 2 stateful op types) drives 12 TMs; JM S tier at 1×2048 MB
        assert result.resource_estimates.total_memory_mb == pytest.approx(51200, rel=1e-2)
        assert result.resource_estimates.total_cpus == 48
        assert result.cluster_recommendations.jobmanager.count == 1
        assert result.cluster_recommendations.jobmanager.memory_mb == 2048
        assert result.cluster_recommendations.jobmanager.total_cpus == 1
        assert result.cluster_recommendations.taskmanagers.count == 12
        assert result.cluster_recommendations.taskmanagers.total_memory_mb == 49152
        assert result.cluster_recommendations.taskmanagers.total_cpus == 47
    
    
    
    def test_moderate_workload(self):
        """Bare_metal-style workers (64GB / 8 CPU), 1.0s latency, 3+10+10 statements, 10M keys — heavy state and CPU."""
        input_params = EstimationInput(
            project_name="Moderate Test",
            messages_per_second=5000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=1.0,
            num_distinct_keys=10_000_000,
            data_skew_risk="low",
            worker_node_memory_mb=65536,
            worker_node_cpu_max=8,
            bandwidth_capacity_gbps= 100, # 100 Gbps
            simple_statements=3,
            medium_statements=10,
            complex_statements=10,
            number_flink_applications=1,
        )
        input_params = _defaulting_input_params(input_params)
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify calculations
        assert result.input_summary.total_statements == 23
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(4.88, rel=1e-2)
        
        # Check moderate workload resources
        assert result.resource_estimates.total_memory_mb > 190000 # 190GB
        assert result.resource_estimates.total_cpus >=28
        assert result.resource_estimates.total_worker_node_needed >= 3
        # Processing load: (3*0.25 + 2*1.0 + 1*1.2) * key_factor(1M keys) = 3.95 * 1.6 = 6.32
        # NOT USED YET assert result.resource_estimates.processing_load_score == pytest.approx(6.32, rel=1e-1)
        
        # TaskManager scaling
        assert result.cluster_recommendations.taskmanagers.count >= 26
        assert result.cluster_recommendations.taskmanagers.total_memory_mb >= 16384*26
        assert result.cluster_recommendations.taskmanagers.total_cpus >= 26*8

        
    def test_high_volume_workload(self):
        """High message rate, 100 statements, 10M keys on fixed 64GB / 8-CPU per worker (bare_metal fields, not VM t-shirt)."""
        input_params = EstimationInput(
            project_name="High Volume Test",
            messages_per_second=50000,
            avg_record_size_bytes=2048,
            worker_node_memory_mb=65536,
            num_distinct_keys=10000000,
            data_skew_risk="low",
            worker_node_cpu_max=8,
            simple_statements=50,
            medium_statements=30,
            complex_statements=20
        )
        input_params = _defaulting_input_params(input_params)
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify high throughput calculations
        assert result.input_summary.total_statements == 100
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(97.66, rel=1e-2)
        
        # High volume should require significant resources
        assert result.resource_estimates.total_memory_mb > 190000 # 190GB
        assert result.resource_estimates.total_cpus >= 27
        assert result.resource_estimates.total_worker_node_needed >= 7
        
        # At least two TMs when CPU or memory requires it (memory may pack into fewer than before)
        assert result.cluster_recommendations.taskmanagers.count >= 2
        

if __name__ == "__main__":
    pytest.main()