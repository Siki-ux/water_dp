import uuid
from unittest.mock import MagicMock

from app.computations.context import ComputationContext
from app.models.alerts import Alert, AlertDefinition
from app.services.alert_evaluator import AlertEvaluator


def test_computation_context_alert(mock_db_session):
    """Test standard active alert creation via Context."""
    script_id = uuid.uuid4()
    ctx = ComputationContext(mock_db_session, "job-1", script_id, {})

    # Mock finding definition
    definition = AlertDefinition(
        id=uuid.uuid4(), target_id=str(script_id), is_active=True, name="Rule 1"
    )

    def query_side_effect(model):
        if model == AlertDefinition:
            m = MagicMock()
            m.filter.return_value.all.return_value = [definition]
            return m
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect

    # Trigger Alert
    ctx.alert("Test Alert", {"val": 100})

    # Verify DB add called
    assert mock_db_session.add.called
    args = mock_db_session.add.call_args[0]
    alert = args[0]
    assert isinstance(alert, Alert)
    assert alert.message == "Test Alert"
    assert alert.definition_id == definition.id


def test_alert_evaluator_passive_alert(mock_db_session):
    """Test passive alert evaluation based on result."""
    script_id = uuid.uuid4()

    # Rule: Trigger if "risk_score" > 50
    definition = AlertDefinition(
        id=uuid.uuid4(),
        target_id=str(script_id),
        is_active=True,
        name="High Risk",
        conditions={"field": "risk_score", "operator": ">", "value": 50},
    )

    evaluator = AlertEvaluator(mock_db_session)

    # Mock query
    def query_side_effect(model):
        if model == AlertDefinition:
            m = MagicMock()
            m.filter.return_value.all.return_value = [definition]
            return m
        if model == Alert:
            # Handle deduplication check: evaluator.db.query(Alert).filter(...).first()
            m = MagicMock()
            m.filter.return_value.first.return_value = None
            return m
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect

    # Evaluate Result - Should Trigger
    result = {"risk_score": 80, "status": "success"}
    evaluator.evaluate_result("job-1", script_id, result)

    assert mock_db_session.add.called
    alert = mock_db_session.add.call_args[0][0]
    assert "triggered" in alert.message.lower() or "80" in alert.message

    # Evaluate Result - Should NOT Trigger
    mock_db_session.reset_mock()
    result_low = {"risk_score": 10}
    evaluator.evaluate_result("job-2", script_id, result_low)
    assert not mock_db_session.add.called
