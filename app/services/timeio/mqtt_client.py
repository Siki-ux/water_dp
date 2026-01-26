"""
MQTT Client for TimeIO

Handles MQTT message publishing for data ingestion.
Uses the TimeIO topic structure: mqtt_ingest/<username>/<suffix>
"""

import json
import logging
from typing import Any, Dict, Optional

from app.core.config import settings

logger = logging.getLogger(__name__)

# Try to import paho-mqtt, fall back to HTTP-based publishing if not available
try:
    import paho.mqtt.client as mqtt

    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False
    logger.warning("paho-mqtt not installed, using HTTP fallback for MQTT publishing")


class MQTTClient:
    """
    MQTT client for publishing observations to TimeIO.

    Supports both direct MQTT (via paho-mqtt) and HTTP-based publishing
    (via a proxy endpoint) for environments where MQTT is not directly accessible.
    """

    def __init__(
        self,
        broker_host: str = None,
        broker_port: int = 1883,
        use_http_fallback: bool = False,
    ):
        """
        Initialize MQTT client.

        Args:
            broker_host: MQTT broker hostname
            broker_port: MQTT broker port
            use_http_fallback: If True, use HTTP proxy instead of direct MQTT
        """
        self.broker_host = broker_host or getattr(
            settings, "mqtt_broker_host", "mqtt-broker"
        )
        self.broker_port = broker_port
        self.use_http_fallback = use_http_fallback or not PAHO_AVAILABLE

    def publish_message(
        self,
        topic: str,
        payload: Any,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> bool:
        """
        Publish a generic message to the MQTT broker.

        Args:
            topic: MQTT topic
            payload: JSON-serializable payload
            username: Optional username (default: settings.mqtt_username)
            password: Optional password (default: settings.mqtt_password)
        """
        if not isinstance(payload, str):
            payload = json.dumps(payload)

        user = username or getattr(settings, "mqtt_username", None)
        pw = password or getattr(settings, "mqtt_password", None)

        logger.info(f"Publishing message to {topic}")

        if self.use_http_fallback:
            return self._publish_via_http(topic, payload, user, pw)
        else:
            return self._publish_direct(topic, payload, user, pw)

    def publish_observation(
        self,
        mqtt_username: str,
        mqtt_password: str,
        data: Dict[str, Any],
        topic_suffix: str = "data",
    ) -> bool:
        """
        Publish observation data to MQTT broker.

        Creates the proper topic structure and payload format expected by
        TimeIO's worker-mqtt-ingest service.

        Args:
            mqtt_username: MQTT username (e.g., "u_z9fxpvh2")
            mqtt_password: MQTT password
            data: Observation data in Chirpstack format:
                  {"time": "ISO8601", "object": {"param1": value, ...}}
            topic_suffix: Topic suffix (default: "data")

        Returns:
            True if published successfully
        """
        topic = f"mqtt_ingest/{mqtt_username}/{topic_suffix}"
        payload = json.dumps(data)

        logger.info(f"Publishing to {topic}: {payload[:100]}...")

        if self.use_http_fallback:
            return self._publish_via_http(topic, payload, mqtt_username, mqtt_password)
        else:
            return self._publish_direct(topic, payload, mqtt_username, mqtt_password)

    def _publish_direct(
        self,
        topic: str,
        payload: str,
        username: str,
        password: str,
    ) -> bool:
        """Publish directly via MQTT protocol."""
        if not PAHO_AVAILABLE:
            logger.error("paho-mqtt not available for direct publishing")
            return False

        try:
            client = mqtt.Client()
            if username and password:
                client.username_pw_set(username, password)
            client.connect(self.broker_host, self.broker_port, keepalive=60)

            # Start loop to handle ACKs
            client.loop_start()

            result = client.publish(topic, payload, qos=1)
            # Wait for publication to complete
            try:
                result.wait_for_publish(timeout=10)
            except RuntimeError:
                # Timeout occurred
                pass

            client.loop_stop()
            client.disconnect()

            if result.is_published():
                logger.info(f"Published to {topic} successfully")
                return True
            else:
                logger.error(f"Failed to publish to {topic} (rc={result.rc})")
                return False

        except Exception as e:
            logger.error(f"MQTT publish error: {e}")
            return False

    def _publish_via_http(
        self,
        topic: str,
        payload: str,
        username: str,
        password: str,
    ) -> bool:
        """
        Publish via HTTP proxy (Docker exec or API).

        This is a fallback for when direct MQTT is not possible.
        Uses docker exec to run mosquitto_pub inside the broker container.
        """
        import subprocess

        try:
            # Try to find MQTT broker container
            result = subprocess.run(
                ["docker", "ps", "--filter", "name=mqtt-broker", "-q"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            container_id = result.stdout.strip()

            if not container_id:
                logger.error("MQTT broker container not found")
                return False

            # Publish via docker exec
            cmd = [
                "docker",
                "exec",
                container_id,
                "mosquitto_pub",
                "-h",
                "localhost",
                "-p",
                "1883",
                "-t",
                topic,
                "-u",
                username,
                "-P",
                password,
                "-m",
                payload,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

            if result.returncode == 0:
                logger.info(f"Published to {topic} via HTTP fallback")
                return True
            else:
                logger.error(f"mosquitto_pub failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            logger.error("MQTT publish timed out")
            return False
        except Exception as e:
            logger.error(f"HTTP fallback publish error: {e}")
            return False

    def build_chirpstack_payload(
        self,
        timestamp: str,
        values: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Build a Chirpstack-compatible payload.

        Args:
            timestamp: ISO8601 timestamp
            values: Dict of parameter name -> value

        Returns:
            Properly formatted payload for chirpstack_generic parser
        """
        return {
            "time": timestamp,
            "object": values,
        }

    def health_check(self) -> bool:
        """Check if MQTT broker is accessible."""
        if not PAHO_AVAILABLE:
            return False

        try:
            client = mqtt.Client()
            client.connect(self.broker_host, self.broker_port, keepalive=5)
            client.disconnect()
            return True
        except Exception:
            return False
