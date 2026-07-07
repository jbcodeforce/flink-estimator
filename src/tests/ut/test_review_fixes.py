"""
Regression tests for sizing correctness at the worker-node packing layer.

Covers: the CPU term in worker-node packing (a recommended fleet must physically carry
total_cpus); reload of estimations saved by the pre-redesign persistence format; whole-unit
JobManager placement; TM/disk colocation (RocksDB state is node-local); and TM-count growth
when the network-buffer heuristic exceeds the per-TM memory cap.
"""

import math

import pytest

from flink_estimator.estimation import (
    calculate_flink_estimation,
    _network_buffer_min_process_memory_mb,
    _pack_worker_nodes,
    CP_FLINK_NODE_CORES,
)
from flink_estimator.models import EstimationInput, SavedEstimation


class TestCpuBoundsWorkerNodes:
    """The worker-node count must include a CPU term: nodes × cores/node >= total_cpus."""

    def test_core_poor_shape_provisions_enough_cores(self):
        """Bare-metal 64 GB / 8-core workers with a ~220-core workload: RAM packs into few
        fat nodes, but the fleet must still be large enough to physically hold the cores."""
        result = calculate_flink_estimation(
            EstimationInput(
                project_name="CPU shortfall regression",
                messages_per_second=524288,
                avg_record_size_bytes=1024,
                num_distinct_keys=1000,
                simple_statements=10,
                medium_statements=0,
                complex_statements=0,
                worker_node_memory_mb=65536,
                worker_node_cpu_max=8,
                worker_node_disk_gb=512,
            )
        )
        re = result.resource_estimates
        assert re.total_cpus > 100  # sanity: this is a big CPU workload
        assert re.provisioned_cores >= re.total_cpus
        assert re.total_worker_node_needed >= math.ceil(
            re.total_cpus / result.input_summary.worker_node_cpu_capacity
        )
        diag = result.sizing_diagnostics
        assert diag.worker_node_bounding_factor == "cpu"
        assert diag.cpu_nodes >= diag.ram_nodes
        assert diag.cpu_nodes >= diag.disk_nodes

    def test_provisioned_cores_never_below_total_cpus_with_fat_tms(self):
        """Few fat TMs (high cores_per_tm) on 64-core workers concentrate a large core need
        onto a small RAM footprint; the fleet must still provision the full compute need."""
        result = calculate_flink_estimation(
            EstimationInput(
                project_name="Fat TM CPU floor",
                messages_per_second=2_000_000,
                avg_record_size_bytes=2048,
                expected_latency_seconds=0.5,
                num_distinct_keys=1000,
                simple_statements=1,
                medium_statements=0,
                complex_statements=0,
                cores_per_tm=64,
                worker_node_memory_mb=256 * 1024,
                worker_node_cpu_max=64,
                worker_node_disk_gb=4096,
            )
        )
        re = result.resource_estimates
        assert re.provisioned_cores >= re.total_cpus


class TestLegacySavedEstimationReload:
    """Files saved by the pre-redesign model (no cp_flink_nodes / provisioned_cores /
    total_disk_gb, old sizing_diagnostics shape) must still validate and reload."""

    @staticmethod
    def _legacy_payload() -> dict:
        """A faithful pre-redesign saved estimation."""
        return {
            "metadata": {
                "estimation_id": "abcd1234",
                "timestamp": "2026-06-01T10:00:00",
                "project_name": "Legacy",
                "saved_at": "2026-06-01 10:00:00",
            },
            "input_parameters": {
                "project_name": "Legacy",
                "messages_per_second": 150000,
                "avg_record_size_bytes": 2000,
                "number_flink_applications": 1,
                "num_distinct_keys": 50_000_000,
                "data_skew_risk": "low",
                "bandwidth_capacity_gbps": 10,
                "expected_latency_seconds": 60.0,
                "simple_statements": 1,
                "medium_statements": 2,
                "complex_statements": 3,
                "worker_node_memory_mb": 65536,
                "worker_node_cpu_max": 16,
                "nb_worker_nodes": 1,
                "worker_node_type": "VM",
                "worker_node_t_size": "M",
            },
            "estimation_results": {
                "input_summary": {
                    "messages_per_second": 150000,
                    "avg_record_size_bytes": 2000,
                    "total_throughput_mb_per_sec": 286.1,
                    "num_distinct_keys": 50_000_000,
                    "data_skew_risk": "low",
                    "bandwidth_capacity_mbps": 10000,
                    "expected_latency_seconds": 60.0,
                    "simple_statements": 1,
                    "medium_statements": 2,
                    "complex_statements": 3,
                    "total_statements": 6,
                    "worker_node_memory_capacity_mb": 65536.0,
                    "worker_node_cpu_capacity": 16,
                    "nb_worker_nodes": 1,
                    "worker_node_t_size": "M",
                },
                "resource_estimates": {
                    "total_memory_mb": 1247232,
                    "total_cpus": 1168,
                    "total_worker_node_needed": 20,
                    "processing_load_score": 1.0,
                },
                "cluster_recommendations": {
                    "jobmanager": {"count": 1, "memory_mb": 4096, "total_cpus": 2.0},
                    "taskmanagers": {
                        "count": 292,
                        "total_memory_mb": 1196032,
                        "total_cpus": 1166,
                        "memory_mb_each": 4096.0,
                    },
                },
                "scaling_recommendations": {
                    "min_parallelism": 584,
                    "recommended_parallelism": 1168,
                    "max_parallelism": 2336,
                    "checkpointing_interval_ms": 60000,
                },
                "capacity_analysis": {
                    "total_flink_statements": 6,
                    "total_flink_applications": 1,
                },
                "sizing_diagnostics": {
                    "nb_tm_state": 292,
                    "nb_tm_cpu": 7,
                    "raw_flink_process_mb": 1192550.4,
                    "tm_process_memory_mb": 4096,
                    "buffer_mb": 1024,
                    "tm_count_bounding_factor": "memory",
                    "total_cpu_bounding_factor": "tm_slots",
                    "per_tm_memory_bounding_factor": "state",
                },
            },
            "version": "1.0",
        }

    def test_legacy_payload_validates(self):
        saved = SavedEstimation(**self._legacy_payload())
        re = saved.estimation_results.resource_estimates
        assert re.total_cpus == 1168
        # Derived where the legacy file has no value.
        assert re.cp_flink_nodes == math.ceil(1168 / CP_FLINK_NODE_CORES)
        assert re.total_disk_gb == 0
        assert re.provisioned_cores == 20 * 16  # nodes × legacy worker_node_cpu_max
        # Legacy diagnostics use a different schema: dropped rather than misrendered.
        assert saved.estimation_results.sizing_diagnostics is None

    def test_new_format_untouched_by_migration(self):
        """The migration must not fire for current-format results."""
        result = calculate_flink_estimation(
            EstimationInput(project_name="Roundtrip", worker_node_type="VM", worker_node_t_size="S")
        )
        payload = self._legacy_payload()
        payload["estimation_results"] = result.model_dump()
        saved = SavedEstimation(**payload)
        assert saved.estimation_results.resource_estimates == result.resource_estimates
        assert saved.estimation_results.sizing_diagnostics == result.sizing_diagnostics


class TestJobManagerWholeUnitPacking:
    """JobManager memory must be placed as whole JMs; per-node leftover fragments that are
    individually too small for a JM cannot be pooled to hold one."""

    def test_second_jm_that_fits_nowhere_adds_a_node(self):
        """S nodes (15 872 MB usable), 3 TMs × 4 096 MB on node 1 leave 3 584 MB — no 8 192 MB
        JM fits there; an empty S node holds only one JM, so 2 JMs need 2 extra nodes."""
        input_params = EstimationInput(
            project_name="JM whole units",
            worker_node_type="VM",
            worker_node_t_size="S",
            number_flink_applications=2,
        )
        pack = _pack_worker_nodes(
            input_params,
            nb_taskmanagers=3,
            per_tm_mem_mb=4096,
            jm_memory=8192,
            required_disk_gb=10,
            total_cores=4.0,
        )
        assert pack["ram_nodes"] == 3

    def test_jm_fits_in_tm_node_leftover_adds_nothing(self):
        """When the leftover on a TM node genuinely holds the JM, no extra node is added."""
        input_params = EstimationInput(
            project_name="JM fits leftover",
            worker_node_type="VM",
            worker_node_t_size="S",
            number_flink_applications=1,
        )
        pack = _pack_worker_nodes(
            input_params,
            nb_taskmanagers=2,
            per_tm_mem_mb=4096,
            jm_memory=2048,
            required_disk_gb=10,
            total_cores=3.0,
        )
        assert pack["ram_nodes"] == 1

    def test_jm_larger_than_node_is_rejected(self):
        input_params = EstimationInput(
            project_name="JM too big",
            worker_node_type="VM",
            worker_node_t_size="S",
        )
        with pytest.raises(ValueError):
            _pack_worker_nodes(
                input_params,
                nb_taskmanagers=1,
                per_tm_mem_mb=4096,
                jm_memory=20480,
                required_disk_gb=0,
                total_cores=2.0,
            )


class TestDiskNodesHaveTaskManagers:
    """RocksDB state is node-local: a node only contributes usable state disk if a
    TaskManager runs on it, so the TM count must cover the disk-node count."""

    def test_tm_count_covers_disk_nodes(self):
        """Tiny throughput (≈1 core) with ~5.5 TB of state on 512 GB nodes: the TM count must
        rise to the disk-node count so each node's NVMe is reachable."""
        result = calculate_flink_estimation(
            EstimationInput(
                project_name="Disk colocation",
                messages_per_second=100,
                avg_record_size_bytes=2000,
                num_distinct_keys=500_000_000,
                simple_statements=0,
                medium_statements=2,
                complex_statements=2,
                worker_node_type="VM",
                worker_node_t_size="S",
            )
        )
        diag = result.sizing_diagnostics
        assert diag.disk_nodes > 1  # sanity: state spans many nodes
        assert result.cluster_recommendations.taskmanagers.count >= diag.disk_nodes


class TestBufferDemandGrowsTmCount:
    """When the network/in-flight buffer heuristic needs more than the per-TM memory cap,
    the estimator must split into more TMs (per-TM buffers shrink with N) instead of
    silently truncating the reported RAM."""

    def test_reported_per_tm_memory_covers_buffer_heuristic(self):
        """One giant TM (cores_per_tm=2000) at 0.5 s latency needs ~120 GB of buffers — more
        than the 64 GB worker cap. The estimator must split into enough TMs that the buffer
        heuristic fits the reported per-TM memory, not halve the RAM figure."""
        input_params = EstimationInput(
            project_name="Buffer cap growth",
            messages_per_second=20_000_000,
            avg_record_size_bytes=2048,
            expected_latency_seconds=0.5,
            num_distinct_keys=1000,
            simple_statements=1,
            medium_statements=0,
            complex_statements=0,
            cores_per_tm=2000,
            worker_node_memory_mb=65536,
            worker_node_cpu_max=64,
            worker_node_disk_gb=4096,
        )
        result = calculate_flink_estimation(input_params)
        tm = result.cluster_recommendations.taskmanagers
        buffer_at_final_count = _network_buffer_min_process_memory_mb(
            input_params, input_params.total_throughput_mb_per_sec, tm.count
        )
        assert tm.count > 1  # the single giant TM was split
        assert tm.memory_mb_each >= buffer_at_final_count

    def test_unsatisfiable_buffer_overhead_is_rejected(self):
        """The buffer heuristic has a fixed per-TM overhead term that grows with the statement
        count and does not shrink as TMs are added. When that floor alone exceeds what a worker
        node can host, no TM count works — the estimator must reject the input instead of
        splitting into millions of TMs or silently truncating the RAM figure."""
        input_params = EstimationInput(
            project_name="Unsatisfiable buffer floor",
            messages_per_second=1000,
            avg_record_size_bytes=512,
            num_distinct_keys=1000,
            simple_statements=80,
            medium_statements=0,
            complex_statements=0,
            number_flink_applications=100,  # total_statements = 8000 -> per-TM floor > 16 GB node
            worker_node_type="VM",
            worker_node_t_size="S",
        )
        with pytest.raises(ValueError):
            calculate_flink_estimation(input_params)


class TestDiskFreeWorkload:
    """A fully stateless workload needs no state disk and no disk-driven nodes."""

    def test_stateless_workload_has_zero_disk_nodes(self):
        result = calculate_flink_estimation(
            EstimationInput(
                project_name="Stateless",
                messages_per_second=5000,
                avg_record_size_bytes=512,
                simple_statements=2,
                medium_statements=0,
                complex_statements=0,
                worker_node_type="VM",
                worker_node_t_size="S",
            )
        )
        assert result.resource_estimates.total_disk_gb == 0
        assert result.sizing_diagnostics.disk_nodes == 0


if __name__ == "__main__":
    pytest.main()
