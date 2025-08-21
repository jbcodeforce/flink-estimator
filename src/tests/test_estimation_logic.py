"""
Unit tests for Flink resource estimation logic.

This module tests the calculate_flink_estimation function with various
scenarios including different workload sizes, complexity levels, and edge cases.
"""

import pytest
import math
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import calculate_flink_estimation


class TestBasicEstimation:
    """Test basic estimation scenarios with typical workloads."""
    
    def test_minimal_workload(self):
        """Test estimation with minimal resource requirements."""
        input_params = EstimationInput(
            project_name="Minimal Test",
            messages_per_second=100,
            avg_record_size_bytes=256,
            simple_statements=1,
            medium_statements=0,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Verify basic structure
        assert result.input_summary.total_statements == 1
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(0.02, rel=1e-1)
        
        # Check resource estimates are reasonable for minimal load
        assert result.resource_estimates.total_memory_mb >= 512
        assert result.resource_estimates.total_cpu_cores >= 2
        # Processing load now includes key distribution factor: 1.2 * 2.0 = 2.4 for 100k keys
        assert result.resource_estimates.processing_load_score == pytest.approx(2.4, rel=1e-1)
        
        # Verify cluster recommendations
        assert result.cluster_recommendations.taskmanagers.count == 1
        assert result.cluster_recommendations.jobmanager.cpu_cores == 1
        
    def test_moderate_workload(self):
        """Test estimation with moderate resource requirements."""
        input_params = EstimationInput(
            project_name="Moderate Test",
            messages_per_second=5000,
            avg_record_size_bytes=1024,
            simple_statements=3,
            medium_statements=2,
            complex_statements=1
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Verify calculations
        assert result.input_summary.total_statements == 6
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(4.88, rel=1e-2)
        
        # Check moderate workload resources
        assert result.resource_estimates.total_memory_mb > 1000
        assert result.resource_estimates.total_cpu_cores >= 4
        # Processing load: (3*1.2 + 2*2.0 + 1*3.5) * 2.0 = 11.1 * 2.0 = 22.2
        assert result.resource_estimates.processing_load_score == pytest.approx(22.2, rel=1e-1)
        
        # TaskManager scaling
        assert result.cluster_recommendations.taskmanagers.count >= 2
        assert result.cluster_recommendations.taskmanagers.memory_mb_each >= 2048
        
    def test_high_volume_workload(self):
        """Test estimation with high-volume requirements."""
        input_params = EstimationInput(
            project_name="High Volume Test",
            messages_per_second=50000,
            avg_record_size_bytes=2048,
            simple_statements=5,
            medium_statements=3,
            complex_statements=2
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Verify high throughput calculations
        assert result.input_summary.total_statements == 10
        assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(97.66, rel=1e-2)
        
        # High volume should require significant resources
        assert result.resource_estimates.total_memory_mb > 5000
        assert result.resource_estimates.total_cpu_cores > 8
        # Processing load: (5*1.2 + 3*2.0 + 2*3.5) * 2.0 (key factor) = 19.0 * 2.0 = 38.0
        assert result.resource_estimates.processing_load_score == pytest.approx(38.0, rel=1e-1)
        
        # Multiple TaskManagers needed
        assert result.cluster_recommendations.taskmanagers.count > 2
        

class TestComplexityScenarios:
    """Test different statement complexity combinations."""
    
    def test_simple_statements_only(self):
        """Test workload with only simple statements."""
        input_params = EstimationInput(
            project_name="Simple Only",
            messages_per_second=10000,
            avg_record_size_bytes=512,
            simple_statements=10,
            medium_statements=0,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Simple statements: (10 * 1.2) * 2.0 (key distribution factor) = 12.0 * 2.0 = 24.0
        expected_load = (10 * 1.2) * 2.0
        assert result.resource_estimates.processing_load_score == pytest.approx(expected_load, rel=1e-1)
        
        # Should have reasonable but not excessive resources
        assert result.resource_estimates.total_cpu_cores >= 4
        assert result.scaling_recommendations.min_parallelism >= 1
        
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
        
        # Complex statements: (5 * 3.5) * 2.0 (key distribution factor) = 17.5 * 2.0 = 35.0
        expected_load = (5 * 3.5) * 2.0
        assert result.resource_estimates.processing_load_score == pytest.approx(expected_load, rel=1e-1)
        
        # Complex processing should require more CPU
        assert result.resource_estimates.total_cpu_cores >= 6
        
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
        assert relaxed_result.scaling_recommendations.checkpointing_interval_ms > 15000
        
    def test_mixed_complexity(self):
        """Test workload with mixed statement complexities."""
        input_params = EstimationInput(
            project_name="Mixed Complexity",
            messages_per_second=2000,
            avg_record_size_bytes=1024,
            simple_statements=4,     # 4 * 1.2 = 4.8
            medium_statements=3,     # 3 * 2.0 = 6.0
            complex_statements=2     # 2 * 3.5 = 7.0
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Total load = (4.8 + 6.0 + 7.0) * 2.0 (key factor) = 17.8 * 2.0 = 35.6
        expected_load = (4 * 1.2 + 3 * 2.0 + 2 * 3.5) * 2.0
        assert result.resource_estimates.processing_load_score == pytest.approx(expected_load, rel=1e-1)
        
        # Mixed complexity should balance resources
        assert result.resource_estimates.total_cpu_cores >= 6
        assert result.cluster_recommendations.taskmanagers.count >= 2


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
        
        # Should use minimal resources (now includes skew factor of 1.3 for medium skew)
        assert result.resource_estimates.total_cpu_cores <= 15
        
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
        assert result.resource_estimates.total_cpu_cores > 4
        assert result.resource_estimates.total_cpu_cores <= 25
        
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
        
        # High throughput requires many resources
        assert result.resource_estimates.total_cpu_cores > 10
        assert result.cluster_recommendations.taskmanagers.count >= 3


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
        assert result.resource_estimates.processing_load_score == 0.0
        
        # Still needs base resources for I/O
        assert result.resource_estimates.total_cpu_cores >= 2
        assert result.cluster_recommendations.taskmanagers.count >= 2
        
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
        assert result.resource_estimates.total_cpu_cores >= 4


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
        
        # TaskManager memory should be between 2GB and 8GB
        tm_memory = result.cluster_recommendations.taskmanagers.memory_mb_each
        assert tm_memory >= 2048  # 2GB minimum
        assert tm_memory <= 8192  # 8GB maximum
        
    def test_taskmanager_cpu_limits(self):
        """Test TaskManager CPU allocation stays within bounds."""
        input_params = EstimationInput(
            project_name="CPU Limits",
            messages_per_second=100000,
            avg_record_size_bytes=1024,
            simple_statements=15,
            medium_statements=10,
            complex_statements=5
        )
        
        result = calculate_flink_estimation(input_params)
        
        # TaskManager CPU should be between 2 and 8 cores
        tm_cpu = result.cluster_recommendations.taskmanagers.cpu_cores_each
        assert tm_cpu >= 2  # 2 cores minimum
        assert tm_cpu <= 8  # 8 cores maximum
        
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
        
        # JobManager should have fixed specs
        jm = result.cluster_recommendations.jobmanager
        assert jm.cpu_cores == 2  # Fixed at 2 cores
        assert jm.memory_mb >= 1024  # At least 1GB
        assert jm.memory_mb <= 4096  # At most 4GB


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
        
        total_cpu = result.resource_estimates.total_cpu_cores
        scaling = result.scaling_recommendations
        
        # Check parallelism relationships
        assert scaling.min_parallelism == max(1, total_cpu // 2)
        assert scaling.recommended_parallelism == total_cpu
        assert scaling.max_parallelism == total_cpu * 2
        
        # Verify order
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
        
        # Complex workload should have longer checkpointing interval
        assert complex_result.scaling_recommendations.checkpointing_interval_ms > \
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
    assert result1.resource_estimates.total_cpu_cores == result2.resource_estimates.total_cpu_cores
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
            "bandwidth_capacity_mbps": 1000,
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
        
        # High skew should require more resources than medium, which should require more than low
        assert high_result.resource_estimates.total_memory_mb > medium_result.resource_estimates.total_memory_mb
        assert medium_result.resource_estimates.total_memory_mb > low_result.resource_estimates.total_memory_mb
        
        assert high_result.resource_estimates.total_cpu_cores > medium_result.resource_estimates.total_cpu_cores
        assert medium_result.resource_estimates.total_cpu_cores > low_result.resource_estimates.total_cpu_cores
        
        # High skew should have more conservative parallelism relative to total CPU cores
        high_parallelism_ratio = high_result.scaling_recommendations.recommended_parallelism / high_result.resource_estimates.total_cpu_cores
        medium_parallelism_ratio = medium_result.scaling_recommendations.recommended_parallelism / medium_result.resource_estimates.total_cpu_cores
        low_parallelism_ratio = low_result.scaling_recommendations.recommended_parallelism / low_result.resource_estimates.total_cpu_cores
        
        # High skew should be more conservative (lower ratio) than medium and low
        assert high_parallelism_ratio <= medium_parallelism_ratio
        assert high_parallelism_ratio <= low_parallelism_ratio
        
        # High skew should have limited max parallelism (equal to total cores, not 2x)
        assert high_result.scaling_recommendations.max_parallelism == high_result.resource_estimates.total_cpu_cores
        assert medium_result.scaling_recommendations.max_parallelism == medium_result.resource_estimates.total_cpu_cores * 2
    
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
        low_bandwidth = EstimationInput(**base_params, bandwidth_capacity_mbps=100)  # Only 100 Mbps
        low_bw_result = calculate_flink_estimation(low_bandwidth)
        
        # High bandwidth that won't be a bottleneck
        high_bandwidth = EstimationInput(**base_params, bandwidth_capacity_mbps=10000)  # 10 Gbps
        high_bw_result = calculate_flink_estimation(high_bandwidth)
        
        # Low bandwidth should require more CPU due to compression/optimization overhead
        assert low_bw_result.resource_estimates.total_cpu_cores >= high_bw_result.resource_estimates.total_cpu_cores
        
        # Both should have the same processing load score (bandwidth doesn't affect this)
        assert low_bw_result.resource_estimates.processing_load_score == high_bw_result.resource_estimates.processing_load_score
    
    def test_distinct_keys_impact(self):
        """Test that number of distinct keys affects resource allocation."""
        base_params = {
            "project_name": "Keys Test",
            "messages_per_second": 10000,
            "avg_record_size_bytes": 1024,
            "data_skew_risk": "medium",
            "bandwidth_capacity_mbps": 1000,
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
        
        # More keys should require more memory for state management
        assert many_keys_result.resource_estimates.total_memory_mb > few_keys_result.resource_estimates.total_memory_mb
        
        # Processing load should be affected by key distribution factor
        assert many_keys_result.resource_estimates.processing_load_score >= few_keys_result.resource_estimates.processing_load_score
    
    def test_input_summary_includes_new_fields(self):
        """Test that input summary includes all new fields."""
        input_params = EstimationInput(
            project_name="Summary Test",
            messages_per_second=5000,
            avg_record_size_bytes=1024,
            num_distinct_keys=250000,
            data_skew_risk="high",
            bandwidth_capacity_mbps=500,
            simple_statements=1,
            medium_statements=1,
            complex_statements=1
        )
        
        result = calculate_flink_estimation(input_params)
        
        # Verify all new fields are in the summary
        assert result.input_summary.num_distinct_keys == 250000
        assert result.input_summary.data_skew_risk == "high"
        assert result.input_summary.bandwidth_capacity_mbps == 500
        
        # Verify existing fields still work
        assert result.input_summary.messages_per_second == 5000
        assert result.input_summary.avg_record_size_bytes == 1024
        assert result.input_summary.total_statements == 3


def test_calculation_properties(sample_estimation_input):
    """Test mathematical properties of the estimation calculation."""
    result = calculate_flink_estimation(sample_estimation_input)
    
    # Total TaskManager resources should equal sum of individual TMs
    tm = result.cluster_recommendations.taskmanagers
    assert tm.total_memory_mb == tm.count * tm.memory_mb_each
    assert tm.total_cpu_cores == tm.count * tm.cpu_cores_each
    
    # Throughput calculation should match manual calculation
    expected_throughput = (sample_estimation_input.messages_per_second * 
                          sample_estimation_input.avg_record_size_bytes) / (1024 * 1024)
    assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-3)
