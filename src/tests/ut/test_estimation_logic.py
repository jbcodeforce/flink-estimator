"""
Unit tests: public `calculate_flink_estimation` API and EstimationInput validation (no private _helpers).

Covers: statement mix and complexity; throughput tiers on bare_metal; TM/CPU invariants; edge message
shapes; scaling (parallelism, checkpoint); Pydantic errors; repeatability; skew and bandwidth
(summary + scaling; current engine does not fold skew/bandwidth into total_cpus); nb_worker_nodes on summary vs total_worker_node_needed.

Uses bare_metal / VM only where noted. Pairs with test_basic_estimation (internal helpers, VM T-shirt E2E).
"""

import pytest
from pydantic import ValidationError
from flink_estimator.models import EstimationInput
from flink_estimator.estimation import calculate_flink_estimation


class TestComplexityScenarios:
    """Complexity-only and mixed op counts; processing_load_score is currently fixed at 1.0 in the engine."""

    def test_complex_statements_only(self):
        """Five complex ops: 1.0s vs 10.0s latency — only <=1.0s gets the parallelism boost (default model latency is 5.0s, so set explicitly). Checkpoint 9000 ms for this branch."""
        input_params = EstimationInput(
            project_name="Complex Only",
            messages_per_second=10000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=1.0,
            simple_statements=0,
            medium_statements=0,
            complex_statements=5
        )
        
        result = calculate_flink_estimation(input_params)
        print(result.model_dump_json(indent=2))        
        assert result.resource_estimates.total_cpus >= 25
        
        relaxed_input = EstimationInput(
            project_name="Complex Only Relaxed",
            messages_per_second=10000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=10.0,
            simple_statements=0,
            medium_statements=0,
            complex_statements=5
        )
        relaxed_result = calculate_flink_estimation(relaxed_input)
        print(relaxed_result.model_dump_json(indent=2))
        # 1.0s expected_latency applies a parallelism multiplier vs 10.0s (see _compute_scaling_recommendations)
        assert result.scaling_recommendations.recommended_parallelism > relaxed_result.scaling_recommendations.recommended_parallelism
        assert result.scaling_recommendations.checkpointing_interval_ms == pytest.approx(9000, rel=0.01)
        
    def test_mixed_complexity(self):
        """4 simple + 3 medium + 2 complex; throughput/CPU path (not a weighted 0.25/1.0/1.2 load score in API)."""
        input_params = EstimationInput(
            project_name="Mixed Complexity",
            messages_per_second=2000,
            avg_record_size_bytes=1024,
            simple_statements=4,
            medium_statements=3,
            complex_statements=2,
        )
       
        result = calculate_flink_estimation(input_params)
        
        assert result.resource_estimates.processing_load_score == pytest.approx(1.0)
        
        assert result.resource_estimates.total_cpus >= 4
        assert result.cluster_recommendations.taskmanagers.count >= 1


class TestThroughputScalingDefaultProfile:
    """Throughput tiers with default (bare_metal) worker memory/CPU — not VM T-shirt E2E."""
    
    def test_low_throughput(self):
        """500 msg/s, 128-byte records; throughput MB/s and coarse CPU upper bound (default low skew, bare_metal)."""
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
        
        assert result.resource_estimates.total_cpus <= 40

    def test_medium_throughput(self):
        """~9.77 MB/s; total_cpus in a mid band (estimator does not apply skew to resource_estimates.total_cpus)."""
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
        
        assert result.resource_estimates.total_cpus > 4
        assert result.resource_estimates.total_cpus <= 35

    def test_high_throughput(self):
        """~195 MB/s; many CPUs; at least one TM for placement."""
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
    """Extreme throughput with one stateless op: TM aggregate CPUs stay at or below resource_estimates.total_cpus."""

    def test_cluster_taskmanager_cpu_covers_workload_estimate(self):
        """~512 MB/s, one simple op, 10M keys, low skew: TM total_cpus is capped by cluster math vs. resource line."""
        # 524288 * 1024 B/s / (1024*1024) = 512 MB/s
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
    """Zero statements, huge records, and tiny records at 1M msg/s."""

    def test_no_statements(self):
        """0+0+0 statements: still at least one TM; processing_load_score placeholder stays 1.0."""
        input_params = EstimationInput(
            project_name="No Statements",
            messages_per_second=1000,
            avg_record_size_bytes=1024,
            simple_statements=0,
            medium_statements=0,
            complex_statements=0
        )
        
        result = calculate_flink_estimation(input_params)
        
        assert result.input_summary.total_statements == 0
        assert result.resource_estimates.processing_load_score == pytest.approx(1.0)
        
        assert result.resource_estimates.total_cpus >= 1
        assert result.cluster_recommendations.taskmanagers.count >= 1
        
    def test_single_large_message(self):
        """10 msg/s, 10 MiB per record: ~100 MB/s; total_memory_mb is material."""
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
        
        assert result.resource_estimates.total_memory_mb > 500
        
    def test_many_small_messages(self):
        """1e6 msg/s, 10-byte records: ~9.54 MB/s; still needs CPU for the one simple op path."""
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
        
        assert result.resource_estimates.total_cpus >= 4


class TestResourceConstraints:
    """Sanity bounds on TM per-process memory (MB) and non-zero TM/JM CPUs."""

    def test_taskmanager_memory_limits(self):
        """memory_mb_each is in MB (typical 4096 per TM when uniform); assert positive and not above raw worker cap + 1 (loose bound)."""
        input_params = EstimationInput(
            project_name="Memory Limits",
            messages_per_second=50000,
            avg_record_size_bytes=4096,
            simple_statements=10,
            medium_statements=5,
            complex_statements=3
        )
        
        result = calculate_flink_estimation(input_params)

        tm_per = result.cluster_recommendations.taskmanagers.memory_mb_each
        assert tm_per >= 2
        assert tm_per <= input_params.worker_node_memory_mb + 1

    def test_taskmanager_cpu_limits(self):
        """Heavy mixed workload: at least one TM core in the cluster line."""
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
        """Non-trivial job: JM has at least 0.5 CPU (units) and >= 1024 MB memory."""
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
        assert jm.total_cpus >= 0.5
        assert jm.memory_mb >= 1024


class TestScalingRecommendations:
    """Invariants on scaling_recommendations (min ≤ recommended ≤ max; checkpoint range across complexity)."""

    def test_parallelism_scaling(self):
        """10.0s latency avoids the <=1.0s boost; ordering of min/recommended/max holds."""
        input_params = EstimationInput(
            project_name="Parallelism Test",
            messages_per_second=20000,
            avg_record_size_bytes=1024,
            expected_latency_seconds=10.0,
            simple_statements=4,
            medium_statements=2,
            complex_statements=1
        )
        
        result = calculate_flink_estimation(input_params)
        
        scaling = result.scaling_recommendations

        assert scaling.min_parallelism <= scaling.recommended_parallelism
        assert scaling.recommended_parallelism <= scaling.max_parallelism
        
    def test_checkpointing_interval(self):
        """10s latency, default low skew: 2 simple vs 5 complex; checkpoint (ms) for complex >= simple, both in [5000, 60000] (may be equal if same skew branch)."""
        simple_input = EstimationInput(
            project_name="Simple Checkpointing",
            messages_per_second=1000,
            avg_record_size_bytes=512,
            expected_latency_seconds=10.0,
            simple_statements=2,
            medium_statements=0,
            complex_statements=0
        )
        
        simple_result = calculate_flink_estimation(simple_input)
        
        complex_input = EstimationInput(
            project_name="Complex Checkpointing",
            messages_per_second=1000,
            avg_record_size_bytes=512,
            expected_latency_seconds=10.0,
            simple_statements=0,
            medium_statements=0,
            complex_statements=5
        )
        
        complex_result = calculate_flink_estimation(complex_input)
        
        assert complex_result.scaling_recommendations.checkpointing_interval_ms >= \
               simple_result.scaling_recommendations.checkpointing_interval_ms

        assert simple_result.scaling_recommendations.checkpointing_interval_ms >= 5000
        assert complex_result.scaling_recommendations.checkpointing_interval_ms <= 60000


class TestInputValidation:
    """Pydantic ValidationError on bad fields; VM model_validator; VM S overwrites memory/CPU."""

    def test_invalid_project_name(self):
        """Whitespace-only project_name fails field validator."""
        with pytest.raises(ValidationError, match="Project name cannot be empty"):
            EstimationInput(
                project_name="   ",  # Only whitespace
                messages_per_second=1000,
                avg_record_size_bytes=1024,
                simple_statements=1
            )
            
    def test_zero_messages_per_second(self):
        """messages_per_second must be > 0."""
        with pytest.raises(ValidationError):
            EstimationInput(
                project_name="Test",
                messages_per_second=0,  # Must be > 0
                avg_record_size_bytes=1024,
                simple_statements=1
            )
            
    def test_zero_record_size(self):
        """avg_record_size_bytes must be > 0."""
        with pytest.raises(ValidationError):
            EstimationInput(
                project_name="Test",
                messages_per_second=1000,
                avg_record_size_bytes=0,  # Must be > 0
                simple_statements=1
            )
            
    def test_negative_statements(self):
        """Statement counts are >= 0."""
        with pytest.raises(ValidationError):
            EstimationInput(
                project_name="Test",
                messages_per_second=1000,
                avg_record_size_bytes=1024,
                simple_statements=-1  # Must be >= 0
            )

    def test_vm_requires_t_size(self):
        """worker_node_type=VM requires worker_node_t_size (model)."""
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
        """bare_metal may omit t_size."""
        inp = EstimationInput(
            project_name="Test",
            messages_per_second=1000,
            avg_record_size_bytes=1024,
            simple_statements=1,
            worker_node_type="bare_metal",
            worker_node_t_size=None,
        )
        assert inp.worker_node_t_size is None

    def test_vm_model_validator_overrides_memory_and_cpu_from_t_size(self):
        """VM T-shirt: model_validator replaces any ad-hoc worker memory/CPU (see VM_TSHIRT in basic)."""
        from flink_estimator.models import VM_TSHIRT_MB_CPU

        mb, cpus = VM_TSHIRT_MB_CPU["S"]
        inp = EstimationInput(
            project_name="SKU",
            messages_per_second=100,
            avg_record_size_bytes=100,
            worker_node_type="VM",
            worker_node_t_size="S",
            worker_node_memory_mb=1.0,
            worker_node_cpu_max=99,
        )
        assert inp.worker_node_memory_mb == mb
        assert inp.worker_node_cpu_max == cpus


# Fixture for common test data
@pytest.fixture
def sample_estimation_input():
    """Reusable 5k msg/s, 1k bytes, 3+2+1 statements for idempotence and summary checks."""
    return EstimationInput(
        project_name="Sample Test Project",
        messages_per_second=5000,
        avg_record_size_bytes=1024,
        simple_statements=3,
        medium_statements=2,
        complex_statements=1
    )


def test_estimation_consistency(sample_estimation_input):
    """Two runs with the same input: same memory, CPUs, and placeholder processing_load_score."""
    result1 = calculate_flink_estimation(sample_estimation_input)
    result2 = calculate_flink_estimation(sample_estimation_input)
    
    assert result1.resource_estimates.total_memory_mb == result2.resource_estimates.total_memory_mb
    assert result1.resource_estimates.total_cpus == result2.resource_estimates.total_cpus
    assert result1.resource_estimates.processing_load_score == result2.resource_estimates.processing_load_score


class TestDataSkewAndBandwidth:
    """Skew changes scaling (e.g. max_parallelism); resource_estimates.total_cpus unchanged; bandwidth in input_summary; key count in scaling."""

    def test_data_skew_impact(self):
        """Identical workload, low/medium/high skew: same total_cpus; high skew caps max_parallelism below medium in this engine."""
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
        
        assert high_result.resource_estimates.total_cpus == medium_result.resource_estimates.total_cpus
        assert medium_result.resource_estimates.total_cpus == low_result.resource_estimates.total_cpus

        assert high_result.scaling_recommendations.max_parallelism <= medium_result.scaling_recommendations.max_parallelism
        assert high_result.scaling_recommendations.max_parallelism <= high_result.resource_estimates.total_cpus
    
    def test_bandwidth_utilization_uses_megabits_per_second(self):
        """input_summary.bandwidth_capacity_mbps = round(gbps*1000); 100 vs 99 Gbps does not change total_cpus in the current estimator (regression on accidental CPU coupling)."""
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
        """100 vs 10000 Gbps field: total_cpus and processing_load currently match (bandwidth is not in the CPU path)."""
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

        low_bandwidth = EstimationInput(**base_params, bandwidth_capacity_gbps=100)
        low_bw_result = calculate_flink_estimation(low_bandwidth)

        high_bandwidth = EstimationInput(**base_params, bandwidth_capacity_gbps=10000)
        high_bw_result = calculate_flink_estimation(high_bandwidth)
        
        assert low_bw_result.resource_estimates.total_cpus == high_bw_result.resource_estimates.total_cpus
        assert low_bw_result.resource_estimates.processing_load_score == high_bw_result.resource_estimates.processing_load_score
    
    def test_distinct_keys_impact(self):
        """1k vs 10M keys: higher key space raises recommended_parallelism (TM CPU line); placeholder load score unchanged."""
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
        
        assert many_keys_result.scaling_recommendations.recommended_parallelism > few_keys_result.scaling_recommendations.recommended_parallelism
        assert many_keys_result.input_summary.num_distinct_keys == 10_000_000
        assert few_keys_result.input_summary.num_distinct_keys == 1_000
        assert many_keys_result.resource_estimates.processing_load_score == pytest.approx(
            few_keys_result.resource_estimates.processing_load_score
        )
    
    def test_input_summary_includes_new_fields(self):
        """input_summary carries num_distinct_keys, data_skew_risk, bandwidth_capacity_mbps (from gbps*1000), and statement fields."""
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
        
        assert result.input_summary.num_distinct_keys == 250000
        assert result.input_summary.data_skew_risk == "high"
        assert result.input_summary.bandwidth_capacity_mbps == 500_000
        
        assert result.input_summary.messages_per_second == 5000
        assert result.input_summary.avg_record_size_bytes == 1024
        assert result.input_summary.total_statements == 3


def test_nb_worker_nodes_in_input_and_total_worker_node_needed():
    """input_summary keeps requested nb_worker_nodes; total_worker_node_needed is min(nodes_used, nb_worker_nodes) in the engine."""
    tiny = EstimationInput(
        project_name="Floor nodes",
        messages_per_second=100,
        avg_record_size_bytes=128,
        simple_statements=1,
        nb_worker_nodes=40,
    )
    result = calculate_flink_estimation(tiny)
    assert result.input_summary.nb_worker_nodes == 40
    assert result.resource_estimates.total_worker_node_needed == 1


def test_calculation_properties(sample_estimation_input):
    """TM count and CPUs positive; per-TM memory at least 1 GiB; throughput matches mps*record_size formula."""
    result = calculate_flink_estimation(sample_estimation_input)
    
    tm = result.cluster_recommendations.taskmanagers
    assert tm.count >= 1
    assert tm.total_cpus >= 1
    assert tm.total_memory_mb >= tm.count * 1024

    expected_throughput = (sample_estimation_input.messages_per_second * 
                          sample_estimation_input.avg_record_size_bytes) / (1024 * 1024)
    assert result.input_summary.total_throughput_mb_per_sec == pytest.approx(expected_throughput, rel=1e-3)

if __name__ == "__main__":
    pytest.main()