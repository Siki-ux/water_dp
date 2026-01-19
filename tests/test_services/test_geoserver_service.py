from unittest.mock import MagicMock, patch

import pytest

from app.core.exceptions import GeoServerException
from app.schemas.geospatial import LayerPublishRequest
from app.services.geoserver_service import GeoServerService


class TestGeoServerService:
    @pytest.fixture
    def service(self, mock_settings):
        return GeoServerService()

    @patch("app.services.geoserver_service.requests.request")
    def test_test_connection_success(self, mock_request, service):
        mock_response = MagicMock()
        mock_response.json.return_value = {"version": "2.20.0"}
        mock_request.return_value = mock_response
        result = service.test_connection()
        assert result is True

    @patch("app.services.geoserver_service.requests.request")
    def test_test_connection_failure(self, mock_request, service):
        mock_request.side_effect = Exception("Connection Error")
        result = service.test_connection()
        assert result is False

    @patch("app.services.geoserver_service.requests.request")
    def test_create_workspace_new(self, mock_request, service):
        import requests

        # First call checks if workspace exists (404), second creates it
        mock_response_check = MagicMock()
        mock_response_check.status_code = 404
        mock_response_check.raise_for_status.side_effect = (
            requests.exceptions.HTTPError(response=mock_response_check)
        )

        mock_response_create = MagicMock()
        mock_response_create.status_code = 201

        mock_request.side_effect = [mock_response_check, mock_response_create]

        result = service.create_workspace("new_workspace")
        assert result is True
        assert mock_request.call_count == 2

    @patch("app.services.geoserver_service.requests.request")
    def test_create_workspace_existing(self, mock_request, service):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        result = service.create_workspace("existing_workspace")
        assert result is True

    @patch("app.services.geoserver_service.requests.request")
    def test_create_workspace_failure(self, mock_request, service):
        import requests

        # Simulate non-404 error
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "Error"
        )

        mock_request.side_effect = mock_response.raise_for_status.side_effect

        with pytest.raises(GeoServerException):
            service.create_workspace("fail_workspace")

    @patch("app.services.geoserver_service.requests.request")
    def test_publish_layer(self, mock_request, service):
        layer_request = LayerPublishRequest(
            layer_name="test_layer",
            store_name="test_store",
            workspace="test_workspace",
            is_public=True,
        )
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_request.return_value = mock_response

        with patch.object(service, "set_layer_style"):
            result = service.publish_layer(layer_request)

        assert result is True
        assert "featureType" in mock_request.call_args.kwargs["json"]

    @patch("app.services.geoserver_service.requests.request")
    def test_publish_sql_view(self, mock_request, service):
        import requests

        # 1. Check if layer exists (404)
        mock_check = MagicMock()
        mock_check.status_code = 404
        mock_check.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=mock_check
        )

        # 2. Create feature type (201)
        mock_create = MagicMock()
        mock_create.status_code = 201

        mock_request.side_effect = [mock_check, mock_create]

        result = service.publish_sql_view(
            layer_name="view_layer",
            store_name="db_store",
            sql="SELECT * FROM table",
            workspace="ws",
        )

        assert result is True
        assert mock_request.call_count == 2

    @patch("app.services.geoserver_service.requests.request")
    def test_unpublish_layer(self, mock_request, service):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        result = service.unpublish_layer("layer_to_delete")
        assert result is True
        assert mock_request.call_args[0][0] == "DELETE"

    @patch("app.services.geoserver_service.requests.request")
    def test_get_layer_info(self, mock_request, service):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "layer": {
                "name": "test_layer",
                "resource": {"name": "test_store", "srs": "EPSG:4326"},
            }
        }
        mock_request.return_value = mock_response

        info = service.get_layer_info("test_layer")
        assert info.name == "test_layer"
        assert info.srs == "EPSG:4326"

    @patch("app.services.geoserver_service.requests.request")
    def test_get_layers(self, mock_request, service):
        # 1. Get list of layers
        mock_list = MagicMock()
        mock_list.json.return_value = {
            "layers": {"layer": [{"name": "layer1"}, {"name": "layer2"}]}
        }

        # 2. Get info for layer1
        # 3. Get info for layer2
        mock_info = MagicMock()
        mock_info.json.return_value = {
            "layer": {
                "name": "layerX",  # Name will be ignored as we check loop count
                "resource": {"name": "store", "srs": "EPSG:4326"},
            }
        }

        mock_request.side_effect = [mock_list, mock_info, mock_info]

        layers = service.get_layers("ws")
        assert len(layers) == 2

    @patch("app.services.geoserver_service.requests.request")
    def test_get_layers_failure(self, mock_request, service):
        mock_request.side_effect = Exception("Conn Error")

        with pytest.raises(GeoServerException):
            service.get_layers("ws")

    @patch("app.services.geoserver_service.requests.request")
    def test_create_datastore(self, mock_request, service):
        # 1. Check exists -> 404
        mock_check = MagicMock()
        mock_check.status_code = 404

        # 2. Create -> 201
        mock_create = MagicMock()
        mock_create.status_code = 201

        mock_request.side_effect = [mock_check, mock_create]

        result = service.create_datastore(
            "new_store", connection_params={"host": "localhost"}
        )
        assert result is True
        assert mock_request.call_count == 2

    @patch("app.services.geoserver_service.requests.request")
    def test_create_style(self, mock_request, service):
        mock_request.return_value.status_code = 201

        result = service.create_style("my_style", "<sld>...</sld>")
        assert result is True
        assert (
            mock_request.call_args[1]["headers"]["Content-Type"]
            == "application/vnd.ogc.sld+xml"
        )

    @patch("app.services.geoserver_service.requests.get")
    @patch("app.services.geoserver_service.ET.fromstring")
    def test_get_layer_capabilities(self, mock_et, mock_get, service):
        mock_get.return_value.text = "<WMS_Capabilities>...</WMS_Capabilities>"
        mock_get.return_value.content = b"<WMS_Capabilities>...</WMS_Capabilities>"
        # Implementation returns a dict with wms_available=True if successful
        result = service.get_layer_capabilities("my_layer")
        assert result["wms_available"] is True

    def test_generate_wms_url(self, service):
        url = service.generate_wms_url("my_layer", workspace="my_ws", width=500)
        # Check base URL and params
        assert service.wms_url in url
        assert "layers=my_ws:my_layer" in url
        assert "width=500" in url

    def test_generate_wfs_url(self, service):
        url = service.generate_wfs_url("my_layer", workspace="my_ws")
        assert service.wfs_url in url
        assert "typeNames=my_ws:my_layer" in url
