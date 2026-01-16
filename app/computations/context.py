import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from sqlalchemy.orm import Session

from app.models.alerts import Alert, AlertDefinition
from app.services.time_series_service import TimeSeriesService

logger = logging.getLogger(__name__)


class ComputationContext:
    """
    Context object passed to computation scripts.
    Provides secure access to data and alerting capabilities.
    """

    def __init__(
        self, db: Session, job_id: str, script_id: UUID, params: Dict[str, Any]
    ):
        self.db = db
        self.job_id = job_id
        self.script_id = script_id
        self.params = params
        self._alerts_triggered: List[Dict[str, Any]] = []
        self._ts_service = TimeSeriesService(db)

    def get_sensor_data(self, sensor_id: str, limit: int = 1) -> List[Dict[str, Any]]:
        """
        Fetch latest data for a sensor (Thing ID).
        Proxy to TimeSeriesService.
        """
        try:
            # We assume sensor_id is the string ID used in FROST
            # The service handles mapping to internal ID if needed
            data = self._ts_service.get_latest_data(sensor_id)
            if limit > 0 and len(data) > limit:
                return data[:limit]
            return data
        except Exception as e:
            logger.error(f"Context Error fetching data for {sensor_id}: {e}")
            return []

    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """
        Fetch dataset properties (metadata).
        For now, returns the Thing's properties which represents the dataset.
        """
        try:
            thing = self._ts_service.get_station(dataset_id)
            return thing if thing else {}
        except Exception as e:
            logger.error(f"Context Error fetching dataset {dataset_id}: {e}")
            return {}

    def alert(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        severity: str = "warning",
    ):
        """
        Trigger an alert from within the script.
        """
        alert_data = {
            "message": message,
            "details": details or {},
            "severity": severity,
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._alerts_triggered.append(alert_data)
        logger.info(f"Script {self.script_id} triggered alert: {message}")

        # Persist immediately or queue?
        # Ideally we persist immediately so it's recorded even if script crashes later.
        self._persist_alert(message, details, severity)

    def _persist_alert(self, message: str, details: Dict[str, Any], severity: str):
        """
        Internal: Save alert to DB linking to the Script (Definition target).
        Lookup logic finds active AlertDefinitions targeting this script ID.
        """
        try:
            # Find definitions that target this script
            definitions = (
                self.db.query(AlertDefinition)
                .filter(
                    AlertDefinition.target_id == str(self.script_id),
                    AlertDefinition.is_active,
                )
                .all()
            )

            if not definitions:
                logger.warning(
                    f"No active AlertDefinition found for script {self.script_id}. Alert not saved."
                )
                return

            # For now, trigger ALL definitions associated with this script
            # Refinement: Pass a 'rule_name' to ctx.alert to match Definition.name?
            for definition in definitions:
                # Logic to filter by severity could go here
                new_alert = Alert(
                    definition_id=definition.id,
                    message=message,
                    details=details,
                    timestamp=datetime.utcnow(),
                    status="active",
                )
                self.db.add(new_alert)
            self.db.commit()

        except Exception as e:
            logger.error(f"Failed to persist alert: {e}")
            self.db.rollback()
