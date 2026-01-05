"""
GeoServer integration service for geospatial data management.
"""

import logging
import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.auth import HTTPBasicAuth

from app.core.config import settings
from app.schemas.geospatial import (
    GeoServerLayerInfo,
    LayerPublishRequest,
)

logger = logging.getLogger(__name__)


class GeoServerService:
    """Service for GeoServer operations."""

    def __init__(self):
        self.base_url = settings.geoserver_url.rstrip("/")
        self.username = settings.geoserver_username
        self.password = settings.geoserver_password
        self.workspace = settings.geoserver_workspace
        self.auth = HTTPBasicAuth(self.username, self.password)

        # API endpoints
        self.rest_url = f"{self.base_url}/rest"
        self.wms_url = f"{self.base_url}/wms"
        self.wfs_url = f"{self.base_url}/wfs"
        self.wcs_url = f"{self.base_url}/wcs"

    def _make_request(self, method: str, endpoint: str, **kwargs) -> requests.Response:
        """Make HTTP request to GeoServer."""
        endpoint = endpoint.lstrip("/")
        url = f"{self.rest_url}/{endpoint}"
        kwargs.setdefault("auth", self.auth)
        kwargs.setdefault("headers", {"Content-Type": "application/json"})

        try:
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"GeoServer request failed: {e}")
            raise

    def test_connection(self) -> bool:
        """Test connection to GeoServer."""
        try:
            response = self._make_request("GET", "/about/version.json")
            version_info = response.json()
            logger.info(
                f"Connected to GeoServer version: {version_info.get('version', 'unknown')}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GeoServer: {e}")
            return False

    def create_workspace(self, workspace_name: str = None) -> bool:
        """Create workspace if it doesn't exist."""
        workspace_name = workspace_name or self.workspace

        try:
            # Check if workspace exists
            self._make_request("GET", f"/workspaces/{workspace_name}.json")
            logger.info(f"Workspace {workspace_name} already exists")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Workspace doesn't exist, create it
                workspace_data = {
                    "workspace": {"name": workspace_name, "isolated": False}
                }

                self._make_request("POST", "/workspaces.json", json=workspace_data)
                logger.info(f"Created workspace: {workspace_name}")
                return True
            else:
                raise

    def create_datastore(
        self,
        store_name: str,
        store_type: str = "postgis",
        connection_params: Dict[str, Any] = None,
    ) -> bool:
        """Create data store."""
        try:
            # Check if store exists
            self._make_request(
                "GET", f"/workspaces/{self.workspace}/datastores/{store_name}.json"
            )
            logger.info(f"Data store {store_name} already exists")
            return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                # Store doesn't exist, create it
                store_data = {
                    "dataStore": {
                        "name": store_name,
                        "type": store_type,
                        "enabled": True,
                        "connectionParameters": connection_params or {},
                    }
                }

                self._make_request(
                    "POST",
                    f"/workspaces/{self.workspace}/datastores.json",
                    json=store_data,
                )
                logger.info(f"Created data store: {store_name}")
                return True
            else:
                raise

    def publish_layer(self, layer_request: LayerPublishRequest) -> bool:
        """Publish a layer to GeoServer."""
        try:
            # Create layer configuration
            layer_config = {
                "featureType": {
                    "name": layer_request.layer_name,
                    "nativeName": layer_request.layer_name,
                    "title": layer_request.layer_name,
                    "abstract": f"Layer {layer_request.layer_name}",
                    "enabled": True,
                    "advertised": layer_request.is_public,
                    "srs": "EPSG:4326",
                    "nativeSRS": "EPSG:4326",
                    "store": {"@class": "dataStore", "name": layer_request.store_name},
                }
            }

            # Add metadata if provided
            if layer_request.metadata:
                layer_config["featureType"]["metadata"] = layer_request.metadata

            self._make_request(
                "POST",
                f"/workspaces/{layer_request.workspace}/datastores/{layer_request.store_name}/featuretypes.json",
                json=layer_config,
            )

            # Set style if provided
            if layer_request.style_name:
                self.set_layer_style(layer_request.layer_name, layer_request.style_name)

            logger.info(f"Published layer: {layer_request.layer_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to publish layer {layer_request.layer_name}: {e}")
            raise

    def publish_sql_view(
        self,
        layer_name: str,
        store_name: str,
        sql: str,
        workspace: str = None,
        title: str = None,
    ) -> bool:
        """Publish a layer based on a SQL View."""
        workspace = workspace or self.workspace
        title = title or layer_name

        try:
            # Check if layer exists
            try:
                self._make_request(
                    "GET", f"/workspaces/{workspace}/layers/{layer_name}.json"
                )
                logger.info(f"Layer {layer_name} already exists. Skipping.")
                return True
            except requests.exceptions.HTTPError as e:
                if e.response.status_code != 404:
                    raise

            feature_type_config = {
                "featureType": {
                    "name": layer_name,
                    "title": title,
                    "store": {"@class": "dataStore", "name": store_name},
                    "srs": "EPSG:4326",
                    "nativeSRS": "EPSG:4326",
                    "enabled": True,
                    "metadata": {
                        "entry": [
                            {"@key": "cachingEnabled", "$": "false"},
                            {
                                "@key": "JDBC_VIRTUAL_TABLE",
                                "virtualTable": {
                                    "name": layer_name,
                                    "sql": sql,
                                    "geometry": {
                                        "name": "geometry",
                                        "type": "Geometry",
                                        "srid": 4326,
                                    },
                                },
                            },
                        ]
                    },
                }
            }

            logger.info(f"Publishing SQL View layer: {layer_name}")

            # Create the feature type
            self._make_request(
                "POST",
                f"/workspaces/{workspace}/datastores/{store_name}/featuretypes.json",
                json=feature_type_config,
            )

            logger.info(f"Successfully published SQL View layer: {layer_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to publish SQL View layer {layer_name}: {e}")
            raise

    def unpublish_layer(self, layer_name: str, workspace: str = None) -> bool:
        """Unpublish a layer from GeoServer."""
        workspace = workspace or self.workspace

        try:
            self._make_request(
                "DELETE", f"/workspaces/{workspace}/layers/{layer_name}.json"
            )
            logger.info(f"Unpublished layer: {layer_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to unpublish layer {layer_name}: {e}")
            raise

    def set_layer_style(
        self, layer_name: str, style_name: str, workspace: str = None
    ) -> bool:
        """Set style for a layer."""
        workspace = workspace or self.workspace

        try:
            style_config = {"layer": {"defaultStyle": {"name": style_name}}}

            self._make_request(
                "PUT",
                f"/workspaces/{workspace}/layers/{layer_name}.json",
                json=style_config,
            )
            logger.info(f"Set style {style_name} for layer {layer_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to set style for layer {layer_name}: {e}")
            raise

    def create_style(
        self, style_name: str, sld_content: str, workspace: str = None
    ) -> bool:
        """Create a new style in GeoServer."""
        workspace = workspace or self.workspace

        try:
            style_config = {
                "style": {
                    "name": style_name,
                    "filename": f"{style_name}.sld",
                    "format": "sld",
                }
            }

            # Create style
            self._make_request(
                "POST", f"/workspaces/{workspace}/styles.json", json=style_config
            )

            # Upload SLD content
            self._make_request(
                "PUT",
                f"/workspaces/{workspace}/styles/{style_name}.sld",
                data=sld_content,
                headers={"Content-Type": "application/vnd.ogc.sld+xml"},
            )

            logger.info(f"Created style: {style_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create style {style_name}: {e}")
            raise

    def get_layer_info(
        self, layer_name: str, workspace: str = None
    ) -> Optional[GeoServerLayerInfo]:
        """Get layer information from GeoServer."""
        workspace = workspace or self.workspace

        try:
            response = self._make_request(
                "GET", f"/workspaces/{workspace}/layers/{layer_name}.json"
            )
            layer_data = response.json()

            return GeoServerLayerInfo(
                name=layer_data["layer"]["name"],
                title=layer_data["layer"].get("title", layer_name),
                abstract=layer_data["layer"].get("abstract"),
                workspace=workspace,
                store=layer_data["layer"]["resource"]["name"],
                srs=layer_data["layer"]["resource"].get("srs", "EPSG:4326"),
                native_srs=layer_data["layer"]["resource"].get(
                    "nativeSRS", "EPSG:4326"
                ),
                bounds=layer_data["layer"]["resource"].get("nativeBoundingBox", {}),
                metadata=layer_data["layer"].get("metadata"),
            )
        except Exception as e:
            logger.error(f"Failed to get layer info for {layer_name}: {e}")
            return None

    def get_layer_capabilities(
        self, layer_name: str, workspace: str = None
    ) -> Dict[str, Any]:
        """Get layer capabilities (WMS/WFS)."""
        workspace = workspace or self.workspace

        try:
            # WMS GetCapabilities
            wms_params = {
                "service": "WMS",
                "version": "1.3.0",
                "request": "GetCapabilities",
                "layers": f"{workspace}:{layer_name}",
            }

            response = requests.get(self.wms_url, params=wms_params, auth=self.auth)
            response.raise_for_status()

            # Parse XML response
            ET.fromstring(response.content)
            capabilities = {
                "wms_available": True,
                "wms_url": self.wms_url,
                "layer_name": f"{workspace}:{layer_name}",
                "formats": ["image/png", "image/jpeg", "image/gif"],
                "srs": ["EPSG:4326", "EPSG:3857"],
            }

            return capabilities
        except Exception as e:
            logger.error(f"Failed to get capabilities for layer {layer_name}: {e}")
            return {"wms_available": False}

    def get_layers(self, workspace: str = None) -> List[GeoServerLayerInfo]:
        """Get all layers in a workspace."""
        workspace = workspace or self.workspace

        try:
            response = self._make_request("GET", f"/workspaces/{workspace}/layers.json")
            layers_data = response.json()

            layers = []
            layers_list = layers_data.get("layers", {})
            if not layers_list:
                return []

            for layer_info in layers_list.get("layer", []):
                layer_name = layer_info["name"]
                layer_details = self.get_layer_info(layer_name, workspace)
                if layer_details:
                    layers.append(layer_details)

            return layers
        except Exception as e:
            logger.error(f"Failed to get layers for workspace {workspace}: {e}")
            return []

    def generate_wms_url(
        self,
        layer_name: str,
        workspace: str = None,
        bbox: Tuple[float, float, float, float] = None,
        width: int = 256,
        height: int = 256,
        srs: str = "EPSG:4326",
        format: str = "image/png",
    ) -> str:
        """Generate WMS URL for layer."""
        workspace = workspace or self.workspace

        params = {
            "service": "WMS",
            "version": "1.3.0",
            "request": "GetMap",
            "layers": f"{workspace}:{layer_name}",
            "styles": "",
            "crs": srs,
            "width": width,
            "height": height,
            "format": format,
        }

        if bbox:
            params["bbox"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

        param_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{self.wms_url}?{param_string}"

    def generate_wfs_url(
        self,
        layer_name: str,
        workspace: str = None,
        output_format: str = "application/json",
    ) -> str:
        """Generate WFS URL for layer."""
        workspace = workspace or self.workspace

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": f"{workspace}:{layer_name}",
            "outputFormat": output_format,
        }

        param_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{self.wfs_url}?{param_string}"

    def sync_layer_with_database(
        self,
        layer_name: str,
        table_name: str,
        geometry_column: str = "geometry",
        workspace: str = None,
    ) -> bool:
        """
        Sync GeoServer layer with database table.
        DEPRECATED: Use direct datastore creation and publish_layer or publish_sql_view instead.
        """
        workspace = workspace or self.workspace

        try:
            # Create or update data store connection
            store_name = f"{table_name}_store"
            # Hardcoded for now as it was in original
            connection_params = {
                "host": "localhost",
                "port": "5432",
                "database": "water_data",
                "user": "postgres",
                "passwd": "password",
                "dbtype": "postgis",
                "schema": "public",
            }

            self.create_datastore(store_name, "postgis", connection_params)

            # Publish layer
            layer_request = LayerPublishRequest(
                layer_name=layer_name,
                workspace=workspace,
                store_name=store_name,
                is_public=True,
            )

            return self.publish_layer(layer_request)

        except Exception as e:
            logger.error(f"Failed to sync layer {layer_name} with database: {e}")
            raise
