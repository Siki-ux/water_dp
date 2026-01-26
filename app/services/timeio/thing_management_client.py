"""
Thing Management API Client

Client for interacting with the TimeIO thing-management-api service.
Handles authentication, project and thing CRUD, and user sync.
"""

import logging
from typing import Dict, List, Optional

import requests

from app.core.config import settings
from app.services.keycloak_service import KeycloakService

logger = logging.getLogger(__name__)


class ThingManagementClient:
    """
    Client for thing-management-api.

    Provides typed access to Projects, Things, Ingest Types, and user management.
    Automatically handles authentication via Keycloak service tokens.
    """

    def __init__(self, token: str = None, base_url: str = None, timeout: int = 30):
        """
        Initialize Thing Management client.

        Args:
            token: Bearer token for authentication. If None, gets service token.
            base_url: API base URL. If None, uses settings.THING_MANAGEMENT_API_URL.
            timeout: Request timeout in seconds.
        """
        self._token = token
        self.base_url = (base_url or settings.thing_management_api_url).rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()

    @property
    def token(self) -> str:
        """Get authentication token, fetching from Keycloak if needed."""
        if not self._token:
            self._token = KeycloakService.get_service_token()
        return self._token

    def _headers(self) -> Dict[str, str]:
        """Build request headers with authentication."""
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _url(self, path: str) -> str:
        """Build full URL from path."""
        return f"{self.base_url}/{path.lstrip('/')}"

    def _request(
        self,
        method: str,
        path: str,
        data: Dict = None,
        params: Dict = None,
    ) -> Dict:
        """
        Execute HTTP request to thing-management-api.

        Args:
            method: HTTP method
            path: API path
            data: Request body
            params: Query parameters

        Returns:
            Response JSON or empty dict

        Raises:
            requests.HTTPError: On non-2xx response
        """
        url = self._url(path)

        try:
            response = self._session.request(
                method=method,
                url=url,
                json=data,
                params=params,
                headers=self._headers(),
                timeout=self.timeout,
            )
            response.raise_for_status()

            if response.status_code == 204 or not response.content:
                return {}
            return response.json()

        except requests.exceptions.HTTPError as e:
            logger.error(f"Thing Management API request failed: {method} {url} - {e}")
            # Log response body for debugging
            if e.response is not None:
                logger.error(f"Response: {e.response.text[:500]}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Thing Management API request failed: {method} {url} - {e}")
            raise

    # ========== User Sync ==========

    def sync_user(self) -> Dict:
        """
        Sync current user with thing-management-api.

        Ensures the user exists in the thing-management database.
        Should be called before creating things.

        Returns:
            User sync response
        """
        return self._request("POST", "user/sync")

    # ========== Projects ==========

    def list_projects(self) -> List[Dict]:
        """
        List all projects visible to the current user.

        Returns:
            List of project entities
        """
        result = self._request("GET", "project", params={"page": 1, "pageSize": 0})
        # API returns paginated response with 'items' field
        logger.info(f"Project list: {result}")
        if isinstance(result, dict) and "items" in result:
            return result.get("items", [])
        return result if isinstance(result, list) else [result] if result else []

    def get_project(self, project_id: int) -> Optional[Dict]:
        """
        Get project by internal ID.

        Args:
            project_id: Internal project ID (integer)

        Returns:
            Project entity or None
        """
        try:
            return self._request("GET", f"project/{project_id}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_project_by_uuid(
        self, uuid: str, name: str = None, auth_group: str = None
    ) -> Optional[Dict]:
        """
        Find project by UUID (using various matching strategies).

        In thing-management-api, projects are identified by their group name,
        which often corresponds to the authorization_provider_group from water_dp-api,
        with any "UFZ-TSM:" prefix stripped.

        Args:
            uuid: Project UUID (from water_dp-api)
            name: Project name (optional, helps narrow search)
            auth_group: Authorization group name (e.g., "UFZ-TSM:MyProject" or just "MyProject")

        Returns:
            Project entity or None
        """
        projects = self.list_projects()
        logger.info(f"Project list: {projects}")
        # Extract the group name without prefix
        search_names = []
        logger.info(f"auth_group: {auth_group}")
        if auth_group:
            # Strip "UFZ-TSM:" prefix if present
            clean_group = (
                auth_group.replace("UFZ-TSM:", "").replace("ufz-tsm:", "").strip()
            )
            search_names.append(clean_group)
            search_names.append(clean_group.lower())

            # Try to resolve Keycloak Group ID to Name
            try:
                if len(clean_group) > 30:
                    k_group = KeycloakService.get_group(clean_group)
                    if k_group and k_group.get("name"):
                        group_name = k_group.get("name")
                        logger.info(
                            f"Resolved Keycloak Group ID {clean_group} -> {group_name}"
                        )
                        search_names.append(group_name)
                        search_names.append(group_name.lower())

                        # Also add stripped version handling UFZ-TSM prefix
                        stripped_name = (
                            group_name.replace("UFZ-TSM:", "")
                            .replace("ufz-tsm:", "")
                            .strip()
                        )
                        if stripped_name != group_name:
                            search_names.append(stripped_name)
                            search_names.append(stripped_name.lower())
            except Exception as e:
                logger.warning(f"Failed to resolve Keycloak group {clean_group}: {e}")

        if name:
            search_names.append(name)
            search_names.append(name.lower())
        logger.info(f"Search names: {search_names}")
        # First try exact UUID match in properties
        for p in projects:
            logger.info(f"Project: {p}")
            props = p.get("properties", {}) or {}
            if props.get("uuid") == uuid:
                return p

        # Try matching by name or authorization group
        for p in projects:
            p_name = p.get("name", "")
            for search_name in search_names:
                if p_name.lower() == search_name.lower():
                    logger.info(f"Matched project by name: {p_name}")
                    return p

        return None

    def get_project_by_auth_group(self, auth_group_name: str) -> Optional[Dict]:
        """
        Find project by authorization group name.

        In thing-management-api, projects are identified by name which corresponds
        to the Keycloak group name (stripped of any prefix like "UFZ-TSM:").

        Args:
            auth_group_name: Authorization group name (e.g., "UFZ-TSM:MyProject" or "MyProject")

        Returns:
            Project entity or None
        """
        projects = self.list_projects()

        # Strip prefix from auth group name
        clean_name = (
            auth_group_name.replace("UFZ-TSM:", "").replace("ufz-tsm:", "").strip()
        )
        logger.info(f"Looking for project matching auth group: {clean_name}")

        for p in projects:
            p_name = p.get("name", "")
            if p_name.lower() == clean_name.lower():
                logger.info(f"Found project by auth group: {p_name} (id={p.get('id')})")
                return p

        logger.warning(f"No project found for auth group: {clean_name}")
        return None

    # ========== Things ==========

    def list_things(self, project_id: int = None) -> List[Dict]:
        """
        List things, optionally filtered by project.

        Args:
            project_id: Filter by project ID

        Returns:
            List of thing entities
        """
        params = {}
        if project_id:
            params["project_id"] = project_id

        result = self._request("GET", "thing", params=params)

        # API returns paginated response with 'items' field
        if isinstance(result, dict) and "items" in result:
            return result.get("items", [])

        return result if isinstance(result, list) else [result] if result else []

    def get_thing(self, thing_id: int) -> Optional[Dict]:
        """
        Get thing by internal ID.

        Args:
            thing_id: Internal thing ID (integer)

        Returns:
            Thing entity or None
        """
        try:
            return self._request("GET", f"thing/{thing_id}")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_thing_ingest(self, thing_id: int) -> Optional[Dict]:
        """
        Get thing details including ingest configuration (credentials).

        Args:
            thing_id: Internal thing ID (integer)

        Returns:
            Dict containing 'thing', 'mqttIngest', etc.
        """
        try:
            return self._request("GET", f"thing/{thing_id}/ingest")
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

    def get_thing_by_uuid(self, uuid: str) -> Optional[Dict]:
        """
        Find thing by UUID.

        Args:
            uuid: Thing UUID

        Returns:
            Thing entity or None
        """
        things = self.list_things()
        for t in things:
            if t.get("uuid") == uuid:
                return t
        return None

    def create_thing(
        self,
        name: str,
        project_id: int,
        description: str = "",
        mqtt_device_type: str = "chirpstack_generic",
        properties: Dict = None,
    ) -> Dict:
        """
        Create a new thing via thing-management-api /thing/ingest endpoint.

        The API will automatically:
        - Create the thing with MQTT ingest
        - Provision database schema and FROST resources

        Args:
            name: Thing name
            project_id: Parent project ID (integer from thing-management)
            description: Thing description
            mqtt_device_type: Parser type (e.g., "chirpstack_generic", "campbell_cr6")
            properties: Additional properties

        Returns:
            Created thing entity with UUID and MQTT credentials
        """
        # First sync user to ensure they exist
        self.sync_user()

        # Get MQTT device type ID
        mqtt_device_type_id = self.get_mqtt_device_type_id(mqtt_device_type)
        if not mqtt_device_type_id:
            logger.warning(
                f"MQTT device type '{mqtt_device_type}' not found, using default"
            )
            mqtt_device_type_id = 5  # Default to chirpstack_generic (ID 5)

        # Get ingest type ID for MQTT (usually 2)
        ingest_type_id = self.get_ingest_type_id("mqtt")
        if not ingest_type_id:
            ingest_type_id = 2  # Default MQTT ingest type

        # Generate MQTT credentials
        import secrets
        import string

        mqtt_user = f"u_{secrets.token_hex(4)}"
        mqtt_password = f"p_{''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(16))}"

        # Build the /thing/ingest payload
        payload = {
            "thing": {
                "name": name,
                "description": description,
                "ingestTypeId": ingest_type_id,
                "projectId": project_id,
            },
            "ingest": {
                "thingId": -1,  # Will be populated by the API
                "uri": "localhost",
                "topic": f"mqtt_ingest/{mqtt_user}/data",
                "user": mqtt_user,
                "password": mqtt_password,
                "passwordHashed": "",
                "mqttDeviceTypeId": mqtt_device_type_id,
            },
        }

        logger.info(
            f"Creating thing via /thing/ingest: {name} for project {project_id}"
        )

        # Create thing via /thing/ingest endpoint
        result = self._request("POST", "thing/ingest", data=payload)

        # The result might be the thing directly or wrapped
        thing = result.get("thing", result) if isinstance(result, dict) else result

        # Add MQTT credentials to the response for convenience
        if thing and isinstance(thing, dict):
            thing["mqtt"] = {
                "mqtt_user_name": mqtt_user,
                "mqtt_password": mqtt_password,
                "topic": f"mqtt_ingest/{mqtt_user}/data",
            }

        logger.info(f"Thing created: {thing.get('uuid') if thing else 'unknown'}")
        return thing

    def update_thing(self, thing_id: int, data: Dict) -> Dict:
        """
        Update a thing.

        Args:
            thing_id: Thing ID
            data: Fields to update

        Returns:
            Updated thing entity
        """
        self._request("PATCH", f"thing/{thing_id}", data=data)
        return self.get_thing(thing_id)

    def delete_thing(self, thing_id: int) -> bool:
        """
        Delete a thing.

        Note: thing-management-api may not support DELETE.
        Consider using soft-delete or direct DB cleanup.

        Args:
            thing_id: Thing ID

        Returns:
            True if deleted
        """
        try:
            self._request("DELETE", f"thing/{thing_id}")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 405:
                logger.warning("Thing deletion not supported by API")
            return False

    # ========== Ingest Types ==========

    def list_ingest_types(self) -> List[Dict]:
        """
        List available ingest types.

        Returns:
            List of ingest type entities
        """
        result = self._request("GET", "ingest-type")
        return result if isinstance(result, list) else [result] if result else []

    def get_ingest_type_id(self, type_name: str) -> Optional[int]:
        """
        Get ingest type ID by name.

        Args:
            type_name: Ingest type name (e.g., "mqtt", "sftp")

        Returns:
            Ingest type ID or None
        """
        for it in self.list_ingest_types():
            if it.get("name", "").lower() == type_name.lower():
                return it.get("id")
        return None

    # ========== MQTT Device Types ==========

    def list_mqtt_device_types(self) -> List[Dict]:
        """
        List available MQTT device types.

        Returns:
            List of MQTT device type entities
        """
        result = self._request(
            "GET", "mqtt-device-type", params={"page": 1, "pageSize": 0}
        )
        # API returns paginated response with 'items' field
        if isinstance(result, dict) and "items" in result:
            return result.get("items", [])
        return result if isinstance(result, list) else [result] if result else []

    def get_mqtt_device_type_id(self, type_name: str) -> Optional[int]:
        """
        Get MQTT device type ID by name.

        Args:
            type_name: Device type name (e.g., "chirpstack_generic", "campbell_cr6")

        Returns:
            Device type ID or None
        """
        for dt in self.list_mqtt_device_types():
            if dt.get("name", "").lower() == type_name.lower():
                return dt.get("id")
        return None

    # ========== Utility ==========

    def health_check(self) -> bool:
        """Check if thing-management-api is accessible."""
        try:
            self._request("GET", "health")
            return True
        except Exception:
            return False
