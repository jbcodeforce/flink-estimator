"""
Unit tests for Flink resource estimation logic.

This module tests the calculate_flink_estimation function with various
scenarios including different workload sizes, complexity levels, and edge cases.
"""

import pytest
import math
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import(
    calculate_flink_estimation,
     _defaulting_input_params, 
     _assess_nb_taskmanagers,
     _assess_jobmanager_size,
     _assess_free_mem_per_node,
     _greedy_pack_taskmanagers,
     _assess_cpu_needs,
     TM_MEM_MB,
     )
import os

os.environ.pop("FLINK_ESTIMATOR_DEBUG", None)

class TestBasicEstimation:
    """Test basic estimation scenarios with typical workloads."""
    
    def test_vm_default_parameters(self):
        """Test  with VM worker nodes T-shirt size."""
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="S"
        )
        input_params = _defaulting_input_params(input_params)
        assert input_params.worker_node_memory_mb == 16384
        assert input_params.worker_node_cpu_max == 8
        assert input_params.nb_worker_nodes == 3
        # Statement counts come from EstimationInput model defaults, not from _defaulting_input_params
        assert input_params.simple_statements == 2
        assert input_params.medium_statements == 1
        assert input_params.complex_statements == 1
        assert input_params.data_skew_risk == "low"
        assert input_params.bandwidth_capacity_gbps == 10
        assert input_params.expected_latency_seconds == 1.0

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

    def test_jobmanager_size(self):
        """
        Test the number of job managers based on the number of task managers.
        """
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="S"
        )
        input_params = _defaulting_input_params(input_params)
        input_params.num_distinct_keys=10000000
        input_params.avg_record_size_bytes=512
        input_params.complex_statements=1
        input_params.medium_statements=1
        input_params.simple_statements=1
        jm_cpu,jm_memory= _assess_jobmanager_size(input_params)
        print(f"jm_cpu: {jm_cpu}, jm_memory: {jm_memory}")
        assert jm_cpu == 1
        assert jm_memory == 2048
        input_params.num_distinct_keys=100000000
        jm_cpu,jm_memory= _assess_jobmanager_size(input_params)
        print(f"jm_cpu: {jm_cpu}, jm_memory: {jm_memory}")
        assert jm_cpu == 2
        assert jm_memory == 4096
        input_params.num_distinct_keys=1000000000
        jm_cpu,jm_memory= _assess_jobmanager_size(input_params)
        print(f"jm_cpu: {jm_cpu}, jm_memory: {jm_memory}")
        assert jm_cpu == 4
        assert jm_memory == 8192

    def test_free_mem_per_node(self):
        """
        Test the free memory per node.
        """
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="S"
        )
        input_params = _defaulting_input_params(input_params)
        free_mem_per_node, total_mem = _assess_free_mem_per_node(input_params, 2048)
        print(f"free_mem_per_node: {free_mem_per_node}, {total_mem}")
        # first node has one job manager.
        assert free_mem_per_node == [13824,15872,15872]
        assert total_mem >= 45000

    def test_nb_taskmanagers_from_state_size_small_vm(self):
        """
        Minimum task manager is 1 and the mem is state size.
        When state size is above TM heap size, it increase the number of tm.
        """
        # Small VM with small state too.
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="S",
            simple_statements=1,
            num_distinct_keys=1000000,
            avg_record_size_bytes=512,
            complex_statements=0,
            medium_statements=1  # need at least one medium or complex statement to have a state
        )
        jm_memory = 2048
        nb_taskmanagers, node_allocations,nb_jm = _assess_nb_taskmanagers(input_params, jm_memory)
        print(f"nb_taskmanagers: {nb_taskmanagers}, {node_allocations}")
        assert nb_taskmanagers == 1
        assert node_allocations == [0,1,0]
        assert nb_jm == 1
        # increase the state size to have more TM
        print("\n --- increase the state size to 20 million keys at 1k value size to have more TM\n")
        input_params.num_distinct_keys=20000000 # 20 million keys
        input_params.avg_record_size_bytes=1024
        nb_taskmanagers, node_allocations,nb_jm = _assess_nb_taskmanagers(input_params, jm_memory)
        print(f"nb_taskmanagers: {nb_taskmanagers}, {node_allocations}")
        assert nb_taskmanagers == 12
        assert node_allocations == [3,3,3,3]
        assert nb_jm == 1


    def test_nb_taskmanagers_from_state_size_large_vm(self):
        """
        Large VM, big state dur to keys and more flink stateful operators
        """
        # Large VM, state bigger than TM heap size but lot of capacity per host
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="L",
            simple_statements=1,
            num_distinct_keys=20000000, # 20 million keys,
            avg_record_size_bytes=1024,
            complex_statements=1,
            medium_statements=4,
            number_flink_applications=1,
            expected_latency_seconds=5.0,
        )
        jm_memory = 4096
        nb_taskmanagers, node_allocations,nb_jm = _assess_nb_taskmanagers(input_params, jm_memory)
        print(f"nb_taskmanagers: {nb_taskmanagers}, {node_allocations} {nb_jm}")
        # State for L + medium/complex mix rounds up to three 4GB TM slots.
        assert nb_taskmanagers == 60
        assert node_allocations in [[18,21,21],[20,20,20]]
        assert nb_jm == 3  # 24 TM per jm
 

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


    def test_cpu_need_for_throughput(self):
        """
        Test the worker node need for throughput.
        """
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="S",
            simple_statements=1,
            medium_statements=0,
            complex_statements=0,
            avg_record_size_bytes=1024,
            num_distinct_keys=10000000,
            data_skew_risk="low",
            bandwidth_capacity_gbps= 10, # 10 Gbps
            expected_latency_seconds=1.0,
            number_flink_applications=1,
            messages_per_second=5000,
        )
        input_params = _defaulting_input_params(input_params)
        total_throughput_mb_per_sec = input_params.messages_per_second * input_params.avg_record_size_bytes / (1024 * 1024)
        cpu_total, nb_worker_nodes = _assess_cpu_needs(total_throughput_mb_per_sec, input_params, 1)
        print(f"test_cpu_need_for_throughput: {cpu_total} {input_params.nb_worker_nodes}")
        assert cpu_total >= 6 
        assert nb_worker_nodes == 3



    def test_default_settings_estimation(self):
        input_params = EstimationInput(
            project_name="VM Test",
            worker_node_type="VM",
            worker_node_t_size="S"
        )
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))

    def test_minimal_workload(self):
        """5000 msg/s at 512 bytes, 1 simple statement should run at 10k records per second per core"""
        input_params = EstimationInput(
            project_name="Minimal Test",
            messages_per_second=5000,
            avg_record_size_bytes=512,
            num_distinct_keys=10000000, # 10 million keys
            data_skew_risk="low",
            bandwidth_capacity_gbps= 10000, # 10 Gbps
            expected_latency_seconds=5.0,
            worker_node_type="VM",
            worker_node_t_size="S",
            simple_statements=1,   # stateless filtering
            medium_statements=0,   # deduplication, group by aggregation
            complex_statements=0   # full left join
        )
        state_size = math.ceil(input_params.num_distinct_keys * input_params.avg_record_size_bytes / (1024 * 1024 ))
        print(f"State size: {state_size} MB")
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify basic structure
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(2.44, rel=1e-1)
        assert result.input_summary.worker_node_memory_capacity_mb == pytest.approx(16384, rel=1e-2)
        assert result.input_summary.worker_node_cpu_capacity == 8

        # Check resource estimates are reasonable for minimal load
        assert result.resource_estimates.total_memory_mb >= 6144  # JM + TM aggregate
        assert result.resource_estimates.total_cpus >= 5  # JM + workload CPUs
        assert result.resource_estimates.total_nodes == pytest.approx(3, rel=1e-1)

        # TM count is driven by the CPU/throughput path and may exceed 1
        assert result.cluster_recommendations.taskmanagers.count >= 1
        tm = result.cluster_recommendations.taskmanagers
        assert tm.total_cpus <= result.resource_estimates.total_cpus
        assert result.cluster_recommendations.jobmanager.cpu_cores == pytest.approx(1, rel=1e-1)

    def test_simple_workload(self):
        """100 msg/s at 512 bytes, 1 statement, 0 medium statements, 0 complex statements"""
        # Worker memory must be large enough to host a TM (4096 MB) per node
        input_params = EstimationInput(
            project_name="Simple_Workload Test",
            messages_per_second=5000,
            avg_record_size_bytes=512,
            num_distinct_keys=10000000, # 10 million keys
            data_skew_risk="low",
            bandwidth_capacity_gbps= 10000, # 10 Gbps
            expected_latency_seconds=1.0,
            worker_node_type="VM",
            worker_node_t_size="L",
            simple_statements=1,   # stateless filtering
            medium_statements=2,   # deduplication, group by aggregation
            complex_statements=2,   # full left join
        )
        input_params = _defaulting_input_params(input_params)
        
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify basic structure
        assert result.input_summary.total_statements == 5
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(2.44, rel=1e-1)
        
        # Check resource estimates are reasonable for a multi-statement workload
        assert result.resource_estimates.total_memory_mb >= 51200
        assert result.resource_estimates.total_cpus >= 45
        # NOT USED YETassert result.resource_estimates.processing_load_score == pytest.approx(0.35, rel=1e-1)
        
        # TM count follows memory and CPU; multiple TMs are valid when total_cpu is split across TMs
        assert result.cluster_recommendations.taskmanagers.count >= 12
        assert result.cluster_recommendations.jobmanager.cpu_cores == pytest.approx(1, rel=1e-1)
        assert result.cluster_recommendations.taskmanagers.total_cpus == pytest.approx(47, rel=1e-1)

    
    def test_medium_work_nodes_with_simple_app(self):
        """Test estimation with medium work nodes and simple application."""
        input_params = EstimationInput(
            project_name="Medium Work Nodes Test",
            messages_per_second=10000,
            avg_record_size_bytes=1024,
            num_distinct_keys=10000000,
            simple_statements=1,
            medium_statements=1,
            complex_statements=1,
            number_flink_applications=1,
            expected_latency_seconds=5.0,
            worker_node_type="VM",
            worker_node_t_size="M",
            bandwidth_capacity_gbps= 10, # 100 Gbps
        )
        input_params = _defaulting_input_params(input_params)
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))
        # Verify basic structure
        assert result.input_summary.total_statements == 3
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(9.77, rel=1e-1)
        assert result.resource_estimates.total_memory_mb  == pytest.approx(6144, rel=1e-2)
        assert result.resource_estimates.total_cpus == 13
        assert result.cluster_recommendations.jobmanager.count == 1
        assert result.cluster_recommendations.jobmanager.memory_mb == 2048
        assert result.cluster_recommendations.jobmanager.cpu_cores == 1
        assert result.cluster_recommendations.taskmanagers.count == 1
        assert result.cluster_recommendations.taskmanagers.total_memory_mb == 4096
        assert result.cluster_recommendations.taskmanagers.total_cpus == 12
    
    
    
    def test_moderate_workload(self):
        """Test estimation with moderate resource requirements.
        driven by latency
        """
        input_params = EstimationInput(
            project_name="Moderate Test",
            messages_per_second=5000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=1.0,
            num_distinct_keys=10000000,
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
        assert result.resource_estimates.total_nodes >= 3
        # Processing load: (3*0.25 + 2*1.0 + 1*1.2) * key_factor(1M keys) = 3.95 * 1.6 = 6.32
        # NOT USED YET assert result.resource_estimates.processing_load_score == pytest.approx(6.32, rel=1e-1)
        
        # TaskManager scaling
        assert result.cluster_recommendations.taskmanagers.count >= 26
        assert result.cluster_recommendations.taskmanagers.total_memory_mb >= 16384*26
        assert result.cluster_recommendations.taskmanagers.total_cpus >= 26*8

        
    def test_high_volume_workload(self):
        """Test estimation with high-volume requirements."""
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
        assert result.resource_estimates.total_nodes >= 7
        
        # At least two TMs when CPU or memory requires it (memory may pack into fewer than before)
        assert result.cluster_recommendations.taskmanagers.count >= 2
        

if __name__ == "__main__":
    pytest.main()