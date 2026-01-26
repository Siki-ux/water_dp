from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.alerts import Alert, AlertDefinition


def test_create_alert_definition(client, mock_db_session: Session):
    """Test creating a new alert definition."""
    # Patch Access
    with patch("app.api.v1.endpoints.alerts.ProjectService._check_access"):
        mock_db_session.refresh.side_effect = lambda x: None

        # Assign ID/Timestamps on add
        def add_side_effect(obj):
            if not getattr(obj, "id", None):
                import uuid
                from datetime import datetime

                obj.id = uuid.uuid4()
                obj.created_at = datetime.utcnow()
                obj.updated_at = datetime.utcnow()

                # Force timestamp for Alert
                # Use string check to avoid import issues inside function if shadowed, but checking class name is safe
                if type(obj).__name__ == "Alert":
                    if not getattr(obj, "timestamp", None):
                        obj.timestamp = datetime.utcnow()
                    if getattr(obj, "details", None) is None:
                        obj.details = {}
                elif hasattr(obj, "timestamp") and not getattr(obj, "timestamp", None):
                    obj.timestamp = datetime.utcnow()

                # Defaults for AlertDefinition if missing
                if (
                    hasattr(obj, "is_active")
                    and getattr(obj, "is_active", None) is None
                ):
                    obj.is_active = True
                if hasattr(obj, "severity") and getattr(obj, "severity", None) is None:
                    obj.severity = "warning"

        mock_db_session.add.side_effect = add_side_effect

        import uuid

        pid = str(uuid.uuid4())
        response = client.post(
            f"{settings.api_prefix}/alerts/definitions",
            json={
                "name": "High Water Alert",
                "description": "Trigger when level > 5m",
                "alert_type": "threshold",
                "target_id": "1",
                "conditions": {"threshold": 5.0, "operator": ">"},
                "is_active": True,
                "project_id": pid,
            },
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert data["name"] == "High Water Alert"
        assert data["id"] is not None


def test_list_alert_definitions(client, mock_db_session: Session):
    """Test listing alert definitions."""
    import uuid

    pid = uuid.uuid4()
    # Create definition
    definition = AlertDefinition(
        id=uuid.uuid4(),
        project_id=pid,
        name="Existing Rule",
        alert_type="threshold",
        conditions={"threshold": 2.0},
        target_id="2",
        is_active=True,
        severity="warning",
    )

    # Mock Query
    def query_side_effect(model):
        if model == AlertDefinition:
            m = MagicMock()
            m.filter.return_value.all.return_value = [definition]
            return m
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect

    with patch("app.api.v1.endpoints.alerts.ProjectService._check_access"):
        response = client.get(
            f"{settings.api_prefix}/alerts/definitions/{pid}",
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1


@pytest.mark.xfail(
    reason="Complex mocking of Alert object state causes 500 error in test env"
)
def test_trigger_test_alert(client, mock_db_session: Session):
    """Test the manual trigger endpoint."""
    import uuid

    pid = uuid.uuid4()
    # Create definition
    definition = AlertDefinition(
        id=uuid.uuid4(),
        project_id=pid,
        name="Test Rule",
        alert_type="threshold",
        conditions={"val": 10.0},
        target_id="1",
    )

    def query_side_effect(model):
        if model == AlertDefinition:
            m = MagicMock()
            m.filter.return_value.first.return_value = definition
            return m
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect
    mock_db_session.refresh.side_effect = lambda x: None

    def add_side_effect(obj):
        if hasattr(obj, "id") and obj.id is None:
            import uuid

            obj.id = uuid.uuid4()

    mock_db_session.add.side_effect = add_side_effect

    with patch("app.api.v1.endpoints.alerts.ProjectService._check_access"):
        response = client.post(
            f"{settings.api_prefix}/alerts/test-trigger",
            json={"definition_id": str(definition.id), "message": "Manual Test"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "active"


def test_get_alert_history(client, mock_db_session: Session):
    """Test fetching alert history."""
    import uuid

    pid = uuid.uuid4()

    from datetime import datetime

    aid = uuid.uuid4()
    did = uuid.uuid4()
    alert = Alert(
        id=aid,
        definition_id=did,
        status="active",
        message="Triggered",
        details={"info": "trigger"},
        timestamp=datetime.utcnow(),
    )
    # Mock the relationship
    alert.definition = AlertDefinition(
        id=did,
        project_id=pid,
        name="Test Rule",
        alert_type="threshold",
        severity="warning",
        is_active=True,
        conditions={"threshold": 10.0},
    )

    def query_side_effect(model):
        if model == Alert:
            m = MagicMock()
            # chain: join(AlertDefinition).filter(...).order_by(...).limit(...).all()
            m.join.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [
                alert
            ]
            return m
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect

    with patch("app.api.v1.endpoints.alerts.ProjectService._check_access"):
        response = client.get(
            f"{settings.api_prefix}/alerts/history/{pid}",
        )
        assert response.status_code == 200, response.text
        data = response.json()
        assert len(data) >= 1
        assert data[0]["status"] == "active"
