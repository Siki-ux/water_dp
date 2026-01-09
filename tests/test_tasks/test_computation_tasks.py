from unittest.mock import MagicMock, patch

from app.computations import flood_prediction
from app.tasks.computation_tasks import run_computation_task


@patch("app.tasks.computation_tasks.importlib.util.module_from_spec")
@patch("app.tasks.computation_tasks.importlib.util.spec_from_file_location")
@patch("os.path.exists")
def test_run_computation_task_success(
    mock_exists, mock_spec_from_file, mock_module_from_spec
):
    # Setup mock
    mock_exists.return_value = True

    # Mock spec and module
    mock_spec = MagicMock()
    mock_spec.loader = MagicMock()
    mock_spec_from_file.return_value = mock_spec

    mock_module = MagicMock()
    mock_module.run.return_value = {"result": "success"}
    mock_module_from_spec.return_value = mock_module

    # Run task
    result = run_computation_task("mock_script", {})

    # Verify
    mock_spec_from_file.assert_called_once()
    mock_module.run.assert_called_with({})
    assert result == {"result": "success"}


@patch("os.path.exists")
def test_run_computation_task_file_not_found(mock_exists):
    mock_exists.return_value = False

    result = run_computation_task("missing", {})

    assert "error" in result
    assert "not found" in result["error"]


@patch("app.tasks.computation_tasks.importlib.util.spec_from_file_location")
@patch("os.path.exists")
def test_run_computation_task_import_error(mock_exists, mock_spec_from_file):
    mock_exists.return_value = True
    # Simulate failed spec loading
    mock_spec_from_file.return_value = None

    result = run_computation_task("broken", {})

    assert "error" in result
    assert "Could not load spec" in result["error"]


def test_flood_prediction_script():
    # Run the actual script logic
    params = {"location_id": 123}

    # We don't want to wait 5 seconds in test, so we patch time.sleep
    with patch("time.sleep"):
        result = flood_prediction.run(params)

    assert result["location_id"] == 123
    assert "risk_score" in result
    assert "prediction" in result
    assert result["prediction"] in ["FLOOD", "NORMAL"]
