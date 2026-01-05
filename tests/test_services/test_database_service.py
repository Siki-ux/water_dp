import pytest

from app.models.geospatial import GeoFeature, GeoLayer
from app.schemas.geospatial import (
    GeoFeatureCreate,
    GeoFeatureUpdate,
    GeoLayerCreate,
    GeoLayerUpdate,
)
from app.services.database_service import DatabaseService


class TestDatabaseService:
    @pytest.fixture
    def service(self, mock_db_session):
        return DatabaseService(mock_db_session)

    # GeoServer Tests

    def test_create_geo_layer(self, service, mock_db_session):
        layer_data = GeoLayerCreate(
            layer_name="rivers",
            title="River Layer",
            store_name="water_store",
            workspace="water",
            layer_type="vector",
            geometry_type="line",
            srid="EPSG:4326",  # Schema uses string for geometry type?
        )
        result = service.create_geo_layer(layer_data)
        mock_db_session.add.assert_called_once()
        assert result.layer_name == "rivers"

    def test_get_geo_layers(self, service, mock_db_session):
        mock_query = mock_db_session.query.return_value
        mock_query.filter.return_value = mock_query
        service.get_geo_layers(workspace="water", layer_type="vector")
        assert mock_query.filter.call_count >= 2

    def test_get_geo_layer(self, service, mock_db_session):
        # Service queries by layer_name
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            GeoLayer(layer_name="rivers")
        )
        result = service.get_geo_layer("rivers")
        assert result.layer_name == "rivers"

    def test_update_geo_layer(self, service, mock_db_session):
        mock_layer = GeoLayer(layer_name="rivers", description="Old Desc")
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            mock_layer
        )

        update_data = GeoLayerUpdate(description="New Desc")
        result = service.update_geo_layer("rivers", update_data)
        assert result.description == "New Desc"

    def test_delete_geo_layer(self, service, mock_db_session):
        mock_layer = GeoLayer(layer_name="rivers")
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            mock_layer
        )

        service.delete_geo_layer("rivers")
        mock_db_session.delete.assert_called_with(mock_layer)

    def test_create_geo_feature(self, service, mock_db_session):
        # First mock get_geo_layer to return a layer
        mock_layer = GeoLayer(layer_name="rivers")
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            mock_layer
        )

        feature_data = GeoFeatureCreate(
            layer_id="rivers",  # Required by base schema
            feature_id="F1",
            feature_type="line",
            geometry={
                "type": "LineString",
                "coordinates": [[0, 0], [1, 1]],
            },  # Schema wants dict not string
            properties={"name": "Danube"},
        )
        service.create_geo_feature(feature_data)
        mock_db_session.add.assert_called()

    def test_get_geo_features(self, service, mock_db_session):
        mock_query = mock_db_session.query.return_value
        mock_query.join.return_value = mock_query
        mock_query.filter.return_value = mock_query

        service.get_geo_features(
            "rivers", feature_type="vector", is_active=True, bbox="0,0,1,1"
        )
        mock_db_session.query.assert_called()

    def test_get_geo_feature(self, service, mock_db_session):
        mock_feature = GeoFeature(feature_id="F1", layer_id="rivers")
        # Implementation: query(GeoFeature).filter(F_id, L_id).first()
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            mock_feature
        )

        result = service.get_geo_feature("F1", "rivers")
        assert result.feature_id == "F1"

    def test_update_geo_feature(self, service, mock_db_session):
        mock_feature = GeoFeature(feature_id="F1", layer_id="rivers", properties={})
        # Implementation calls specific query, let's just match the return of first()
        # Note: update likely calls get_geo_feature internally or does similar query
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            mock_feature
        )

        update_data = GeoFeatureUpdate(properties={"new": "prop"})
        result = service.update_geo_feature("F1", "rivers", update_data)
        assert result.properties == {"new": "prop"}

    def test_delete_geo_feature(self, service, mock_db_session):
        mock_feature = GeoFeature(feature_id="F1", layer_id="rivers")
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            mock_feature
        )

        service.delete_geo_feature("F1", "rivers")
        mock_db_session.delete.assert_called_with(mock_feature)
