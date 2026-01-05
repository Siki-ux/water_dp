from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

from app.models.geospatial import GeoFeature, GeoLayer
from app.models.time_series import TimeSeriesData, TimeSeriesMetadata
from app.models.water_data import WaterStation
from app.schemas.geospatial import (
    GeoFeatureCreate,
    GeoFeatureUpdate,
    GeoLayerCreate,
    GeoLayerUpdate,
)
from app.schemas.time_series import TimeSeriesDataCreate, TimeSeriesMetadataCreate
from app.schemas.water_data import WaterDataPointCreate, WaterStationCreate
from app.services.database_service import DatabaseService


class TestDatabaseService:
    @pytest.fixture
    def service(self, mock_db_session):
        return DatabaseService(mock_db_session)

    def test_create_station_success(self, service, mock_db_session):
        station_data = WaterStationCreate(
            station_id="TEST001",
            name="Test Station",
            latitude=50.0,
            longitude=14.0,
            station_type="river",
            status="active",
        )
        result = service.create_station(station_data)
        mock_db_session.add.assert_called_once()
        mock_db_session.commit.assert_called_once()
        assert result.station_id == "TEST001"

    def test_create_station_failure(self, service, mock_db_session):
        station_data = WaterStationCreate(
            station_id="FAIL001",
            name="Fail Station",
            latitude=0.0,
            longitude=0.0,
            station_type="river",
            status="active",
        )
        mock_db_session.commit.side_effect = Exception("DB Error")
        with pytest.raises(Exception):
            service.create_station(station_data)
        mock_db_session.rollback.assert_called_once()

    def test_get_station(self, service, mock_db_session):
        station_id = "TEST001"
        expected_station = WaterStation(station_id=station_id)
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            expected_station
        )

        result = service.get_station(station_id)
        assert result == expected_station
        mock_db_session.query.assert_called_with(WaterStation)

    def test_get_stations_filtering(self, service, mock_db_session):
        # Test filtering by type and status
        mock_query = mock_db_session.query.return_value
        # Allow chaining of filter calls
        mock_query.filter.return_value = mock_query

        service.get_stations(station_type="river", status="active")

        assert mock_query.filter.call_count == 2

    def test_update_station_success(self, service, mock_db_session):
        station_id = "TEST001"
        existing_station = WaterStation(station_id=station_id, name="Old Name")
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            existing_station
        )

        update_data = {"name": "New Name"}
        result = service.update_station(station_id, update_data)

        assert result.name == "New Name"
        mock_db_session.commit.assert_called()

    def test_update_station_not_found(self, service, mock_db_session):
        mock_db_session.query.return_value.filter.return_value.first.return_value = None
        result = service.update_station("NONEXISTENT", {})
        assert result is None

    def test_create_data_point(self, service, mock_db_session):
        point_data = WaterDataPointCreate(
            station_id=1,
            timestamp=datetime.now(),
            parameter="temperature",
            value=25.5,
            unit="C",
        )
        service.create_data_point(point_data)
        mock_db_session.add.assert_called_once()
        mock_db_session.commit.assert_called_once()

    def test_get_data_points_filtering(self, service, mock_db_session):
        mock_query = mock_db_session.query.return_value
        # Allow chaining
        mock_query.filter.return_value = mock_query

        start = datetime.now() - timedelta(days=1)
        end = datetime.now()

        service.get_data_points(
            station_id=1, start_time=start, end_time=end, parameter="temp"
        )

        # Verify multiple filters were applied (station_id, start, end, parameter) = 4
        assert mock_query.filter.call_count >= 4

    def test_get_latest_data(self, service, mock_db_session):
        # This mocks a complex query involving subqueries
        # We just verify the main query structure is called
        service.get_latest_data(station_id=1, parameter="temp")
        mock_db_session.query.assert_called()

    def test_create_time_series_metadata(self, service, mock_db_session):
        meta_data = TimeSeriesMetadataCreate(
            series_id="TS1",
            name="Test Series",
            station_id="S1",
            parameter="flow",
            sampling_rate="1h",
            unit="m3/s",
            source_type="sensor",
            start_time=datetime.now(),
            data_type="continuous",
        )
        result = service.create_time_series_metadata(meta_data)
        mock_db_session.add.assert_called_once()
        assert result.series_id == "TS1"

    def test_get_time_series_metadata(self, service, mock_db_session):
        service.get_time_series_metadata(
            parameter="flow", source_type="sensor", station_id="S1"
        )
        mock_db_session.query.assert_called()

    def test_get_time_series_metadata_by_id(self, service, mock_db_session):
        mock_db_session.query.return_value.filter.return_value.first.return_value = (
            TimeSeriesMetadata(series_id="TS1")
        )
        result = service.get_time_series_metadata_by_id("TS1")
        assert result.series_id == "TS1"

    def test_get_station_statistics(self, service, mock_db_session):
        # Mocking the result of the complex query for station statistics
        mock_stat = MagicMock()
        mock_stat.parameter = "temp"
        mock_stat.count = 10
        mock_stat.avg_value = 5.0
        mock_stat.min_value = 1.0
        mock_stat.max_value = 9.0
        mock_stat.std_value = 2.0

        mock_db_session.query.return_value.filter.return_value.group_by.return_value.all.return_value = [
            mock_stat
        ]

        stats = service.get_station_statistics(station_id=1)
        assert stats["parameters"][0]["count"] == 10
        assert stats["parameters"][0]["average"] == 5.0

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

    def test_add_time_series_data(self, service, mock_db_session):
        points = [
            TimeSeriesDataCreate(series_id="TS1", timestamp=datetime.now(), value=1.0),
            TimeSeriesDataCreate(series_id="TS1", timestamp=datetime.now(), value=2.0),
        ]
        result = service.add_time_series_data(points)
        mock_db_session.add_all.assert_called_once()
        assert len(result) == 2

    def test_get_time_series_data(self, service, mock_db_session):
        series_id = "TS001"
        mock_query = mock_db_session.query.return_value
        mock_filter = mock_query.filter.return_value
        mock_order = mock_filter.order_by.return_value
        mock_limit = mock_order.limit.return_value

        expected_data = [TimeSeriesData(series_id=series_id, value=10.0)]
        mock_limit.all.return_value = expected_data

        result = service.get_time_series_data(series_id=series_id)
        assert len(result) == 1
