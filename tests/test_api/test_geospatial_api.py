from unittest.mock import MagicMock, patch

from app.core.exceptions import DatabaseException, ResourceNotFoundException


def test_create_geo_layer(client):
    from datetime import datetime

    data = {
        "layer_name": "NewLayer",
        "title": "NL",
        "store_name": "S",
        "workspace": "W",
        "layer_type": "vector",
        "geometry_type": "polygon",
        "srs": "EPSG:4326",
    }

    mock_layer = MagicMock(
        id=1,
        layer_name="NewLayer",
        title="NL",
        store_name="S",
        workspace="W",
        layer_type="vector",
        geometry_type="polygon",
        srs="EPSG:4326",
        is_published=True,
        is_public=False,
        properties=None,
        style_config=None,
        data_source=None,
        data_format=None,
        style_name=None,
        description=None,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )

    with patch("app.api.v1.endpoints.geospatial.DatabaseService") as MockService:
        MockService.return_value.create_geo_layer.return_value = mock_layer

        response = client.post("/api/v1/geospatial/layers", json=data)
        assert response.status_code == 201
        assert response.json()["layer_name"] == "NewLayer"


def test_get_geo_layers(client):
    with patch("app.services.geoserver_service.GeoServerService") as MockService:
        MockService.return_value.get_layers.return_value = []

        response = client.get("/api/v1/geospatial/layers")
        assert response.status_code == 200
        assert response.json()["total"] == 0


def test_get_geo_layer_not_found(client):
    with patch("app.api.v1.endpoints.geospatial.DatabaseService") as MockService:
        MockService.return_value.get_geo_layer.side_effect = ResourceNotFoundException(
            "Not found"
        )

        response = client.get("/api/v1/geospatial/layers/MISSING")
        assert response.status_code == 404


def test_create_geo_feature_error(client):
    data = {
        "feature_id": "F1",
        "layer_id": "L1",
        "feature_type": "V",
        "geometry": {"type": "Point", "coordinates": [0, 0]},
        "properties": {},
    }
    with patch("app.api.v1.endpoints.geospatial.DatabaseService") as MockService:
        MockService.return_value.create_geo_feature.side_effect = DatabaseException(
            "DB Fail"
        )

        response = client.post("/api/v1/geospatial/features", json=data)
        assert response.status_code == 500
        assert response.json()["detail"] == "Database operation failed"
