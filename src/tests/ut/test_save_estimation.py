"""Tests for POST /api/save-estimation."""

import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flink_estimator.estimation import calculate_flink_estimation
from flink_estimator.models import EstimationInput
from main import app


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def sample_input() -> EstimationInput:
    return EstimationInput(
        project_name="save-test",
        messages_per_second=1000,
        avg_record_size_bytes=256,
        worker_node_type="bare_metal",
        worker_node_memory_mb=8192,
        worker_node_cpu_max=8,
        worker_node_disk_gb=512,
        nb_worker_nodes=2,
    )


@pytest.fixture
def sample_result(sample_input):
    return calculate_flink_estimation(sample_input)


class TestSaveEstimationEndpoint:
    def test_save_with_precomputed_results_skips_recalculation(
        self, client, sample_input, sample_result
    ):
        payload = {
            "input_parameters": sample_input.model_dump(),
            "estimation_results": sample_result.model_dump(),
        }
        with patch("main.save_estimation_to_json") as mock_save, patch(
            "main.calculate_flink_estimation"
        ) as mock_calc:
            mock_save.return_value = "test_saved.json"
            response = client.post("/api/save-estimation", json=payload)

        assert response.status_code == 200
        assert response.json()["success"] is True
        mock_calc.assert_not_called()
        mock_save.assert_called_once()
        saved_input, saved_result = mock_save.call_args[0]
        assert saved_input.model_dump() == sample_input.model_dump()
        assert saved_result.model_dump() == sample_result.model_dump()

    def test_save_with_input_only_recalculates(self, client, tmp_path, sample_input):
        with patch(
            "flink_estimator.estimation.SAVED_ESTIMATIONS_DIR", str(tmp_path)
        ):
            response = client.post(
                "/api/save-estimation",
                json={"input_parameters": sample_input.model_dump()},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["filename"].endswith(".json")
        saved_path = tmp_path / data["filename"]
        assert saved_path.exists()
        saved = json.loads(saved_path.read_text())
        assert saved["input_parameters"]["project_name"] == "save-test"
        assert "estimation_results" in saved
