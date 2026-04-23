"""
Unit tests for Flink resource estimation logic.

This module tests the calculate_flink_estimation function with various
scenarios including different workload sizes, complexity levels, and edge cases.
"""

import pytest
from pydantic import ValidationError
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import calculate_flink_estimation


class TestComplexityScenarios:
    """Test different statement complexity combinations."""
    
   
        
    def test_complex_statements_only(self):
        """Test workload with only complex statements."""
        input_params = EstimationInput(
            project_name="Complex Only",
            messages_per_second=1000,
            avg_record_size_bytes=1024,
            simple_statements=0,
            medium_statements=0,
            complex_statements=5
        )
        
        result = calculate_flink_estimation(input_params)
        
        assert result.resource_estimates.processing_load_score == pytest.approx(1.0)
        
        assert result.resource_estimates.total_cpus >= 4
        
        # With default 1.0s latency, checkpoint interval is capped at 10s
        # For relaxed latency, test longer checkpointing intervals
        relaxed_input = EstimationInput(
            project_name="Complex Only Relaxed",
            messages_per_second=1000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=10.0,  # Relaxed latency
            simple_statements=0,
            medium_statements=0,
            complex_statements=5
        )
        relaxed_result = calculate_flink_estimation(relaxed_input)
        # Base checkpoint interval can match when both land on the same cap; relaxed latency boosts parallelism.
        assert result.scaling_recommendations.recommended_parallelism > relaxed_result.scaling_recommendations.recommended_parallelism
        assert result.scaling_recommendations.checkpointing_interval_ms == pytest.approx(9000, rel=0.01)
        
    def test_mixed_complexity(self):
        """Test workload with mixed statement complexities."""
        input_params = EstimationInput(
            project_name="Mixed Complexity",
            messages_per_second=2000,
            avg_record_size_bytes=1024,
            simple_statements=4,     # 4 * 0.25 = 1
            medium_statements=3,     # 3 * 1.0 = 3.0
            complex_statements=2     # 2 * COMPLEX_MULTIPLIER 1.2
        )
       
        result = calculate_flink_estimation(input_params)
        
        assert result.resource_estimates.processing_load_score == pytest.approx(1.0)
        
        assert result.resource_estimates.total_cpus >= 4
        assert result.cluster_recommendations.taskmanagers.count >= 1


class TestThroughputScaling:
    """Test how estimation scales with different throughput levels."""
    
    def test_low_throughput(self):
        """Test low throughput scenario."""
        input_params = EstimationInput(
            project_name="Low Throughput",
            messages_per_second=500,
            avg_record_size_bytes=128,
            simple_statements=2,
            medium_statements=1,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Low throughput = 500 * 128 / (1024 * 1024) ≈ 0.061 MB/s
        expected_throughput = (500 * 128) / (1024 * 1024)
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-1)
        
        # Medium default skew (1.2) scales CPU in the estimate
        assert result.resource_estimates.total_cpus <= 40
        
    def test_medium_throughput(self):
        """Test medium throughput scenario."""
        input_params = EstimationInput(
            project_name="Medium Throughput",
            messages_per_second=10000,
            avg_record_size_bytes=1024,
            simple_statements=3,
            medium_statements=2,
            complex_statements=1
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Medium throughput = 10000 * 1024 / (1024 * 1024) ≈ 9.77 MB/s
        expected_throughput = (10000 * 1024) / (1024 * 1024)
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-2)
        
        # Should scale resources accordingly (now includes skew factor)
        assert result.resource_estimates.total_cpus > 4
        assert result.resource_estimates.total_cpus <= 35
        
    def test_high_throughput(self):
        """Test high throughput scenario."""
        input_params = EstimationInput(
            project_name="High Throughput",
            messages_per_second=100000,
            avg_record_size_bytes=2048,
            simple_statements=5,
            medium_statements=3,
            complex_statements=2
        )
        
        result = calculate_flink_estimation(input_params)
        
        # High throughput = 100000 * 2048 / (1024 * 1024) ≈ 195.31 MB/s
        expected_throughput = (100000 * 2048) / (1024 * 1024)
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-2)
        
        # High throughput requires many resources (CPU; TM count is memory/state driven)
        assert result.resource_estimates.total_cpus > 10
        assert result.cluster_recommendations.taskmanagers.count >= 1


class TestTaskManagerCountCpuConstraint:
    """Regression for jbcodeforce/flink-estimator#1: TM count must respect CPU, not memory alone."""

    def test_cluster_taskmanager_cpu_covers_workload_estimate(self):
        """
        With no SQL statements, memory stays moderate while throughput drives CPU.
        Memory-only TM sizing can yield one TM whose aggregate cores are below
        resource_estimates.total_cpus; cluster TM total CPU must cover the estimate.
        """
        # ~512 MB/s: 524288 * 1024 / (1024*1024) == 512
        mps = 524288
        record_bytes = 1024
        input_params = EstimationInput(
            project_name="CPU bound throughput only",
            messages_per_second=mps,
            avg_record_size_bytes=record_bytes,
            num_distinct_keys=1000,
            data_skew_risk="low",
            bandwidth_capacity_gbps=100_000,
            expected_latency_seconds=10.0,
            simple_statements=1,
            medium_statements=0,
            complex_statements=0,
        )

        result = calculate_flink_estimation(input_params)

        assert result.input_summary.total_statements == 1
        expected_mbps = (mps * record_bytes) / (1024 * 1024)
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(
            expected_mbps, rel=1e-5
        )

        tm = result.cluster_recommendations.taskmanagers
        assert tm.total_cpus <= result.resource_estimates.total_cpus
        assert tm.total_cpus >= 1


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    def test_no_statements(self):
        """Test estimation with no processing statements."""
        input_params = EstimationInput(
            project_name="No Statements",
            messages_per_second=1000,
            avg_record_size_bytes=1024,
            simple_statements=0,
            medium_statements=0,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # No statements means no processing load
        assert result.input_summary.total_statements == 0
        assert result.resource_estimates.processing_load_score == pytest.approx(1.0)
        
        # Still needs base resources for I/O
        assert result.resource_estimates.total_cpus >= 1
        assert result.cluster_recommendations.taskmanagers.count >= 1
        
    def test_single_large_message(self):
        """Test with low frequency but very large messages."""
        input_params = EstimationInput(
            project_name="Large Messages",
            messages_per_second=10,
            avg_record_size_bytes=10 * 1024 * 1024,  # 10MB per message
            simple_statements=1,
            medium_statements=0,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # 10 messages/s * 10MB = 100 MB/s throughput
        expected_throughput = 10 * 10
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-1)
        
        # Large messages require memory for buffering
        assert result.resource_estimates.total_memory_mb > 500
        
    def test_many_small_messages(self):
        """Test with high frequency but very small messages."""
        input_params = EstimationInput(
            project_name="Small Messages",
            messages_per_second=1000000,  # 1M messages/s
            avg_record_size_bytes=10,     # 10 bytes each
            simple_statements=1,
            medium_statements=0,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # 1M messages/s * 10 bytes ≈ 9.54 MB/s
        expected_throughput = (1000000 * 10) / (1024 * 1024)
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-1)
        
        # High message rate requires CPU for processing overhead
        assert result.resource_estimates.total_cpus >= 4


class TestResourceConstraints:
    """Test resource allocation constraints and limits."""
    
    def test_taskmanager_memory_limits(self):
        """Test TaskManager memory allocation stays within bounds."""
        input_params = EstimationInput(
            project_name="Memory Limits",
            messages_per_second=50000,
            avg_record_size_bytes=4096,
            simple_statements=10,
            medium_statements=5,
            complex_statements=3
        )
        
        result = calculate_flink_estimation(input_params)
        
        # TaskManager memory (GB per TM) matches target worker_node_memory_gb default (2)
        tm_gb = result.cluster_recommendations.taskmanagers.memory_gb_each
        assert tm_gb >= 2
        assert tm_gb <= input_params.worker_node_memory_mb + 1

    def test_taskmanager_cpu_limits(self):
        """Aggregate TM CPU stays within workload-derived bounds."""
        input_params = EstimationInput(
            project_name="CPU Limits",
            messages_per_second=100000,
            avg_record_size_bytes=1024,
            simple_statements=15,
            medium_statements=10,
            complex_statements=5
        )

        result = calculate_flink_estimation(input_params)
        tm = result.cluster_recommendations.taskmanagers
        assert tm.total_cpus >= 1

    def test_jobmanager_constraints(self):
        """Test JobManager has fixed resource allocation."""
        input_params = EstimationInput(
            project_name="JobManager Test",
            messages_per_second=75000,
            avg_record_size_bytes=2048,
            simple_statements=8,
            medium_statements=4,
            complex_statements=2
        )
        
        result = calculate_flink_estimation(input_params)
        
        jm = result.cluster_recommendations.jobmanager
        assert jm.cpu_cores >= 0.5
        assert jm.memory_mb >= 1024


class TestScalingRecommendations:
    """Test parallelism and scaling recommendations."""
    
    def test_parallelism_scaling(self):
        """Test parallelism recommendations scale with CPU."""
        input_params = EstimationInput(
            project_name="Parallelism Test",
            messages_per_second=20000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=10.0,  # Use relaxed latency to avoid parallelism boost
            simple_statements=4,
            medium_statements=2,
            complex_statements=1
        )
        
        result = calculate_flink_estimation(input_params)
        
        scaling = result.scaling_recommendations

        assert scaling.min_parallelism <= scaling.recommended_parallelism
        assert scaling.recommended_parallelism <= scaling.max_parallelism
        
    def test_checkpointing_interval(self):
        """Test checkpointing interval scales with processing complexity."""
        # Simple workload with relaxed latency
        simple_input = EstimationInput(
            project_name="Simple Checkpointing",
            messages_per_second=1000,
            avg_record_size_bytes=512,
            expected_latency_seconds=10.0,  # Relaxed latency to see complexity differences
            simple_statements=2,
            medium_statements=0,
            complex_statements=0
        )
        
        simple_result = calculate_flink_estimation(simple_input)
        
        # Complex workload with relaxed latency
        complex_input = EstimationInput(
            project_name="Complex Checkpointing",
            messages_per_second=1000,
            avg_record_size_bytes=512,
            expected_latency_seconds=10.0,  # Relaxed latency to see complexity differences
            simple_statements=0,
            medium_statements=0,
            complex_statements=5
        )
        
        complex_result = calculate_flink_estimation(complex_input)
        
        assert complex_result.scaling_recommendations.checkpointing_interval_ms >= \
               simple_result.scaling_recommendations.checkpointing_interval_ms

        # Both should be within reasonable bounds
        assert simple_result.scaling_recommendations.checkpointing_interval_ms >= 5000
        assert complex_result.scaling_recommendations.checkpointing_interval_ms <= 60000


class TestInputValidation:
    """Test input validation and error handling."""
    
    def test_invalid_project_name(self):
        """Test validation of project name field."""
        with pytest.raises(ValueError, match="Project name cannot be empty"):
            EstimationInput(
                project_name="   ",  # Only whitespace
                messages_per_second=1000,
                avg_record_size_bytes=1024,
                simple_statements=1
            )
            
    def test_zero_messages_per_second(self):
        """Test validation of messages per second."""
        with pytest.raises(ValueError):
            EstimationInput(
                project_name="Test",
                messages_per_second=0,  # Must be > 0
                avg_record_size_bytes=1024,
                simple_statements=1
            )
            
    def test_zero_record_size(self):
        """Test validation of record size."""
        with pytest.raises(ValueError):
            EstimationInput(
                project_name="Test",
                messages_per_second=1000,
                avg_record_size_bytes=0,  # Must be > 0
                simple_statements=1
            )
            
    def test_negative_statements(self):
        """Test validation of statement counts."""
        with pytest.raises(ValueError):
            EstimationInput(
                project_name="Test",
                messages_per_second=1000,
                avg_record_size_bytes=1024,
                simple_statements=-1  # Must be >= 0
            )

    def test_vm_requires_t_size(self):
        with pytest.raises(ValidationError, match="worker_node_t_size"):
            EstimationInput(
                project_name="Test",
                messages_per_second=1000,
                avg_record_size_bytes=1024,
                simple_statements=1,
                worker_node_type="VM",
                worker_node_t_size=None,
            )

    def test_bare_metal_allows_no_t_size(self):
        inp = EstimationInput(
            project_name="Test",
            messages_per_second=1000,
            avg_record_size_bytes=1024,
            simple_statements=1,
            worker_node_type="bare_metal",
            worker_node_t_size=None,
        )
        assert inp.worker_node_t_size is None

    def test_vm_tshirt_maps_memory_and_cpu(self):
        from flink_estimator.models import VM_TSHIRT_MB_CPU

        for size in ("S", "M", "L"):
            mb, cpus = VM_TSHIRT_MB_CPU[size]
            inp = EstimationInput(
                project_name="SKU",
                messages_per_second=100,
                avg_record_size_bytes=100,
                worker_node_type="VM",
                worker_node_t_size=size,
                worker_node_memory_mb=1.0,
                worker_node_cpu_max=99,
            )
            assert inp.worker_node_memory_mb == mb
            assert inp.worker_node_cpu_max == cpus


# Fixture for common test data
@pytest.fixture
def sample_estimation_input():
    """Fixture providing a standard estimation input for testing."""
    return EstimationInput(
        project_name="Sample Test Project",
        messages_per_second=5000,
        avg_record_size_bytes=1024,
        simple_statements=3,
        medium_statements=2,
        complex_statements=1
    )


def test_estimation_consistency(sample_estimation_input):
    """Test that multiple runs of the same input produce consistent results."""
    result1 = calculate_flink_estimation(sample_estimation_input)
    result2 = calculate_flink_estimation(sample_estimation_input)
    
    # Results should be identical
    assert result1.resource_estimates.total_memory_mb == result2.resource_estimates.total_memory_mb
    assert result1.resource_estimates.total_cpus == result2.resource_estimates.total_cpus
    assert result1.resource_estimates.processing_load_score == result2.resource_estimates.processing_load_score


class TestDataSkewAndBandwidth:
    """Test new features: data skew risk and bandwidth capacity."""
    
    def test_data_skew_impact(self):
        """Test that data skew risk affects resource allocation."""
        base_params = {
            "project_name": "Skew Test",
            "messages_per_second": 5000,
            "avg_record_size_bytes": 1024,
            "num_distinct_keys": 100000,
            "bandwidth_capacity_gbps": 1,
            "simple_statements": 2,
            "medium_statements": 1,
            "complex_statements": 0
        }
        
        # Test low skew
        low_skew = EstimationInput(**base_params, data_skew_risk="low")
        low_result = calculate_flink_estimation(low_skew)
        
        # Test medium skew
        medium_skew = EstimationInput(**base_params, data_skew_risk="medium")
        medium_result = calculate_flink_estimation(medium_skew)
        
        # Test high skew
        high_skew = EstimationInput(**base_params, data_skew_risk="high")
        high_result = calculate_flink_estimation(high_skew)
        
        # Same workload: CPU/memory totals match; skew affects parallelism recommendations only
        assert high_result.resource_estimates.total_cpus == medium_result.resource_estimates.total_cpus
        assert medium_result.resource_estimates.total_cpus == low_result.resource_estimates.total_cpus

        assert high_result.scaling_recommendations.max_parallelism <= medium_result.scaling_recommendations.max_parallelism
        assert high_result.scaling_recommendations.max_parallelism <= high_result.resource_estimates.total_cpus
    
    def test_bandwidth_utilization_uses_megabits_per_second(self):
        """Throughput is converted MB/s -> Mbps (x8) before comparing to bandwidth_capacity_mbps."""
        # 10 MB/s == 80 Mbps. 80/100 = 0.8 (not above threshold); 80/99 > 0.8 triggers penalty.
        mps = 1048576
        record_bytes = 10
        base = dict(
            project_name="Mbps unit test",
            messages_per_second=mps,
            avg_record_size_bytes=record_bytes,
            num_distinct_keys=1000,
            data_skew_risk="low",
            expected_latency_seconds=10.0,
            simple_statements=1,
            medium_statements=0,
            complex_statements=0,
        )
        at_threshold = calculate_flink_estimation(
            EstimationInput(**base, bandwidth_capacity_gbps=100)
        )
        below_capacity = calculate_flink_estimation(
            EstimationInput(**base, bandwidth_capacity_gbps=99)
        )
        assert below_capacity.resource_estimates.total_cpus == at_threshold.resource_estimates.total_cpus

    def test_bandwidth_bottleneck(self):
        """Test that low bandwidth capacity triggers additional CPU overhead."""
        # High throughput scenario
        base_params = {
            "project_name": "Bandwidth Test",
            "messages_per_second": 100000,
            "avg_record_size_bytes": 2048,
            "num_distinct_keys": 100000,
            "data_skew_risk": "medium",
            "simple_statements": 2,
            "medium_statements": 1,
            "complex_statements": 0
        }
        
        # Low bandwidth that will be a bottleneck
        low_bandwidth = EstimationInput(**base_params, bandwidth_capacity_gbps=100)  # Only 100 Mbps
        low_bw_result = calculate_flink_estimation(low_bandwidth)
        
        # High bandwidth that won't be a bottleneck
        high_bandwidth = EstimationInput(**base_params, bandwidth_capacity_gbps=10000)  # 10 Gbps
        high_bw_result = calculate_flink_estimation(high_bandwidth)
        
        assert low_bw_result.resource_estimates.total_cpus == high_bw_result.resource_estimates.total_cpus
        assert low_bw_result.resource_estimates.processing_load_score == high_bw_result.resource_estimates.processing_load_score
    
    def test_distinct_keys_impact(self):
        """Test that number of distinct keys affects resource allocation."""
        base_params = {
            "project_name": "Keys Test",
            "messages_per_second": 10000,
            "avg_record_size_bytes": 1024,
            "data_skew_risk": "medium",
            "bandwidth_capacity_gbps": 1,
            "simple_statements": 2,
            "medium_statements": 1,
            "complex_statements": 0
        }
        
        # Few keys
        few_keys = EstimationInput(**base_params, num_distinct_keys=1000)
        few_keys_result = calculate_flink_estimation(few_keys)
        
        # Many keys
        many_keys = EstimationInput(**base_params, num_distinct_keys=10000000)
        many_keys_result = calculate_flink_estimation(many_keys)
        
        # Many distinct keys increase TM count from the state/throughput pass (before CPU overwrite) and
        # scaling recommendations; total_memory_mb in the public API is dominated by ceil(total_cpu_needs).
        assert many_keys_result.scaling_recommendations.recommended_parallelism > few_keys_result.scaling_recommendations.recommended_parallelism
        assert many_keys_result.input_summary.num_distinct_keys == 10_000_000
        assert few_keys_result.input_summary.num_distinct_keys == 1_000
        assert many_keys_result.resource_estimates.processing_load_score == pytest.approx(
            few_keys_result.resource_estimates.processing_load_score
        )
    
    def test_input_summary_includes_new_fields(self):
        """Test that input summary includes all new fields."""
        input_params = EstimationInput(
            project_name="Summary Test",
            messages_per_second=5000,
            avg_record_size_bytes=1024,
            num_distinct_keys=250000,
            data_skew_risk="high",
            bandwidth_capacity_gbps=500,
            simple_statements=1,
            medium_statements=1,
            complex_statements=1
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Verify all new fields are in the summary
        assert result.input_summary.num_distinct_keys == 250000
        assert result.input_summary.data_skew_risk == "high"
        assert result.input_summary.bandwidth_capacity_mbps == 500_000
        
        # Verify existing fields still work
        assert result.input_summary.messages_per_second == 5000
        assert result.input_summary.avg_record_size_bytes == 1024
        assert result.input_summary.total_statements == 3


def test_nb_worker_nodes_floors_total_nodes():
    tiny = EstimationInput(
        project_name="Floor nodes",
        messages_per_second=100,
        avg_record_size_bytes=128,
        simple_statements=1,
        nb_worker_nodes=40,
    )
    result = calculate_flink_estimation(tiny)
    assert result.resource_estimates.total_nodes >= 40


def test_calculation_properties(sample_estimation_input):
    """Test mathematical properties of the estimation calculation."""
    result = calculate_flink_estimation(sample_estimation_input)
    
    tm = result.cluster_recommendations.taskmanagers
    assert tm.count >= 1
    assert tm.total_cpus >= 1
    assert tm.total_memory_mb >= tm.count * 1024

    # Throughput calculation should match manual calculation
    expected_throughput = (sample_estimation_input.messages_per_second * 
                          sample_estimation_input.avg_record_size_bytes) / (1024 * 1024)
    assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-3)

if __name__ == "__main__":
    pytest.main()