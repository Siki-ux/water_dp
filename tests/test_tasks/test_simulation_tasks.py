from unittest.mock import MagicMock, patch
from app.tasks.simulation_tasks import run_simulation_step, process_single_simulation

@patch("app.tasks.simulation_tasks.SessionLocal")
@patch("app.tasks.simulation_tasks.TimeIODatabase")
@patch("app.tasks.simulation_tasks.mqtt.Client")
def test_run_simulation_step(mock_mqtt, mock_timeio, mock_db):
    # Mock DB Session
    session = mock_db.return_value
    mock_sim = MagicMock()
    mock_sim.is_enabled = True
    mock_sim.last_run = None
    mock_sim.thing_uuid = "u1"
    mock_sim.config = [{"name": "s1", "config": {"type": "random"}}]
    session.query.return_value.filter.return_value.all.return_value = [mock_sim]
    
    # Mock TimeIO
    mock_timeio.return_value.get_thing_configs_by_uuids.return_value = {
        "u1": {"mqtt_user": "device1"}
    }
    
    # Run
    run_simulation_step()
    
    # Assert
    mock_mqtt.return_value.connect.assert_called()
    mock_mqtt.return_value.publish.assert_called()
    session.commit.assert_called()

def test_process_single_simulation():
    mock_client = MagicMock()
    config = [{"name": "temp", "config": {"type": "random", "range": {"min": 10, "max": 20}}}]
    mqtt_user = "user1"
    
    process_single_simulation(mock_client, config, mqtt_user)
    
    mock_client.publish.assert_called_once()
    topic = mock_client.publish.call_args[0][0]
    payload = mock_client.publish.call_args[0][1]
    
    assert "mqtt_ingest/user1/data" in topic
    assert "temp" in payload
