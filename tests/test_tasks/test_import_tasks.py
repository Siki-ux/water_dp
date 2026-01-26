from unittest.mock import patch
from app.tasks.import_tasks import import_geojson_task, import_timeseries_task

@patch("app.tasks.import_tasks.os.path.exists")
@patch("app.tasks.import_tasks.os.path.getsize")
@patch("app.tasks.import_tasks.os.remove")
def test_import_geojson_success(mock_remove, mock_getsize, mock_exists):
    mock_exists.return_value = True
    mock_getsize.return_value = 1000
    
    result = import_geojson_task("test.json")
    assert result["status"] == "Mock success"
    mock_remove.assert_called_once_with("test.json")

@patch("app.tasks.import_tasks.os.path.exists")
def test_import_geojson_not_found(mock_exists):
    mock_exists.return_value = False
    result = import_geojson_task("test.json")
    assert result["status"] == "Error"

@patch("app.tasks.import_tasks.os.path.exists")
@patch("app.tasks.import_tasks.os.path.getsize")
@patch("app.tasks.import_tasks.os.remove")
def test_import_timeseries_success(mock_remove, mock_getsize, mock_exists):
    mock_exists.return_value = True
    mock_getsize.return_value = 1000
    
    result = import_timeseries_task("test.csv")
    assert result["status"] == "Mock success"
    mock_remove.assert_called_once_with("test.csv")
