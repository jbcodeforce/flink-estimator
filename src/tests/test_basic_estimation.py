"""
Unit tests for Flink resource estimation logic.

This module tests the calculate_flink_estimation function with various
scenarios including different workload sizes, complexity levels, and edge cases.
"""

import pytest
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import calculate_flink_estimation
import os
os.environ["FLINK_ESTIMATOR_DEBUG"] = "1"

class TestBasicEstimation:
    """Test basic estimation scenarios with typical workloads."""
    

    def test_minimal_workload(self):
        """5000 msg/s at 512 bytes, 1 statement, 0 medium statements, 0 complex statements"""
        input_params = EstimationInput(
            project_name="Minimal Test",
            messages_per_second=5000,
            avg_record_size_bytes=512,
            num_distinct_keys=10000000, # 10 million keys
            data_skew_risk="low",
            bandwidth_capacity_mbps= 10000, # 10 Gbps
            expected_latency_seconds=1.0,
            taskmanager_memory_gb=4.0,
            taskmanager_cpu_max=8,
            simple_statements=1,   # stateless filtering
            medium_statements=0,   # deduplication, group by aggregation
            complex_statements=0   # full left join
        )
        
        result = calculate_flink_estimation(input_params)
        # Verify basic structure
        assert result.input_summary.total_statements == 1
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(2.44, rel=1e-1)
        
        # Check resource estimates are reasonable for minimal load
        assert result.resource_estimates.total_memory_mb >= 8*1024 # MB
        assert result.resource_estimates.total_cpus >= 3 # 1 for job manager and 1.5 for task manager
        
        # TM count follows memory and CPU; multiple TMs are valid when total_cpu is split across TMs
        assert result.cluster_recommendations.taskmanagers.count == 2
        tm = result.cluster_recommendations.taskmanagers
        assert tm.total_cpus <= result.resource_estimates.total_cpus
        assert result.cluster_recommendations.jobmanager.cpu_cores == pytest.approx(1, rel=1e-1)

    def test_simple_workload(self):
        """100 msg/s at 512 bytes, 1 statement, 0 medium statements, 0 complex statements"""
        input_params = EstimationInput(
            project_name="Simple_Workload Test",
            messages_per_second=5000,
            avg_record_size_bytes=512,
            num_distinct_keys=10000000, # 10 million keys
            data_skew_risk="low",
            bandwidth_capacity_mbps= 10000, # 10 Gbps
            expected_latency_seconds=1.0,
            taskmanager_memory_gb=4.0,
            taskmanager_cpu_max=8,
            simple_statements=1,   # stateless filtering
            medium_statements=2,   # deduplication, group by aggregation
            complex_statements=2   # full left join
        )
        
        result = calculate_flink_estimation(input_params)
        # Verify basic structure
        assert result.input_summary.total_statements == 5
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(2.44, rel=1e-1)
        
        # Check resource estimates are reasonable for minimal load
        assert result.resource_estimates.total_memory_mb >= 20480 # 20 GB
        assert result.resource_estimates.total_cpus >= 7 # 6 for TM 1 for JM
        # NOT USED YETassert result.resource_estimates.processing_load_score == pytest.approx(0.35, rel=1e-1)
        
        # TM count follows memory and CPU; multiple TMs are valid when total_cpu is split across TMs
        assert result.cluster_recommendations.taskmanagers.count >= 6
        assert result.cluster_recommendations.jobmanager.cpu_cores == 1

        
    def test_moderate_workload(self):
        """Test estimation with moderate resource requirements."""
        input_params = EstimationInput(
            project_name="Moderate Test",
            messages_per_second=5000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=1.0,
            num_distinct_keys=10000000,
            data_skew_risk="low",
            taskmanager_memory_gb=16.0,
            taskmanager_cpu_max=8,
            bandwidth_capacity_mbps= 100000, # 100 Gbps
            simple_statements=3,
            medium_statements=10,
            complex_statements=10
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Verify calculations
        assert result.input_summary.total_statements == 23
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(4.88, rel=1e-2)
        
        # Check moderate workload resources
        assert result.resource_estimates.total_memory_mb > 190000 # 190GB
        assert result.resource_estimates.total_cpus >=28
        assert result.resource_estimates.total_nodes >= 4
        # Processing load: (3*0.25 + 2*1.0 + 1*1.2) * key_factor(1M keys) = 3.95 * 1.6 = 6.32
        # NOT USED YET assert result.resource_estimates.processing_load_score == pytest.approx(6.32, rel=1e-1)
        
        # TaskManager scaling
        assert result.cluster_recommendations.taskmanagers.count >= 26
        assert result.cluster_recommendations.taskmanagers.memory_gb_each >= 16

        
    def test_high_volume_workload(self):
        """Test estimation with high-volume requirements."""
        input_params = EstimationInput(
            project_name="High Volume Test",
            messages_per_second=50000,
            avg_record_size_bytes=2048,
            taskmanager_memory_gb=32.0,
            num_distinct_keys=10000000,
            data_skew_risk="low",
            taskmanager_cpu_max=8,
            simple_statements=50,
            medium_statements=30,
            complex_statements=20
        )
        
        result = calculate_flink_estimation(input_params)
        
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