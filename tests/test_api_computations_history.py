from unittest.mock import MagicMock, patch

from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.computations import ComputationJob, ComputationScript


def test_list_script_jobs(client, mock_db_session: Session):
    """Test listing execution history for a script."""
    import uuid

    sid = uuid.uuid4()
    pid = uuid.uuid4()

    # Create script
    script = ComputationScript(
        id=sid, project_id=pid, name="Test Script", filename="test.py"
    )
    mock_db_session.add(script)
    mock_db_session.flush()

    # Create job
    job = ComputationJob(
        id="job-123",  # Celery ID
        script_id=script.id,
        user_id="user-1",
        status="SUCCESS",
        start_time="2023-01-01T12:00:00",
        end_time="2023-01-01T12:01:00",
        result='{"risk": 10}',
        logs="Done.",
    )
    mock_db_session.add(job)
    mock_db_session.commit()

    # Mock Query side effects
    def query_side_effect(model):
        if model == ComputationScript:
            m = MagicMock()
            m.filter.return_value.first.return_value = script
            return m
        elif model == ComputationJob:
            m = MagicMock()
            # chain: filter().order_by().limit().all()
            m.filter.return_value.order_by.return_value.limit.return_value.all.return_value = [
                job
            ]
            return m
        return MagicMock()

    mock_db_session.query.side_effect = query_side_effect

    with patch("app.api.v1.endpoints.computations.ProjectService._check_access"):
        response = client.get(
            f"{settings.api_prefix}/computations/jobs/{script.id}",
        )
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "job-123"
        assert data[0]["status"] == "SUCCESS"
        assert data[0]["result"] == '{"risk": 10}'
