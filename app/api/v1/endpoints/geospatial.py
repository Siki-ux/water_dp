"""
Geospatial API endpoints.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.api.deps import get_current_user, has_role
from app.core.config import settings
from app.core.database import get_db
from app.schemas.geospatial import (
    FeatureListResponse,
    GeoFeatureCreate,
    GeoFeatureResponse,
    GeoFeatureUpdate,
    GeoLayerCreate,
    GeoLayerResponse,
    GeoLayerUpdate,
    LayerListResponse,
    LayerPublishRequest,
    LayerUnpublishRequest,
    SpatialQuery,
    SpatialQueryResponse,
)
from app.services.database_service import DatabaseService
from app.services.geoserver_service import GeoServerService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post(
    "/layers",
    response_model=GeoLayerResponse,
    status_code=201,
    dependencies=[Depends(has_role("admin"))],
)
async def create_geo_layer(layer: GeoLayerCreate, db: Session = Depends(get_db)):
    """Create a new geospatial layer."""
    db_service = DatabaseService(db)
    return db_service.create_geo_layer(layer)


@router.get("/layers", response_model=LayerListResponse)
async def get_geo_layers(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    workspace: Optional[str] = Query(None, description="Filter by workspace"),
    layer_type: Optional[str] = Query(None, description="Filter by layer type"),
    is_published: Optional[bool] = Query(
        None, description="Filter by publication status"
    ),
    is_public: Optional[bool] = Query(None, description="Filter by public access"),
    db: Session = Depends(get_db),
):
    """Get geospatial layers with filtering."""
    """Get geospatial layers with filtering (Proxy to GeoServer)."""
    # [USER REQUEST] Get layers from GeoServer, not local DB.
    try:
        from app.services.geoserver_service import GeoServerService

        geoserver_service = GeoServerService()

        # Fetch from GeoServer (default workspace if not specified)
        target_workspace = workspace or settings.geoserver_workspace
        if not target_workspace:
            # If no workspace configured/provided, return empty or fail?
            # Let's try to get from configured default
            target_workspace = "water_data"

        try:
            gs_layers = geoserver_service.get_layers(target_workspace)
        except Exception:
            # Fallback to empty if connection fails
            gs_layers = []

        # Map GeoServerLayerInfo to GeoLayerResponse (Mocking DB fields)
        mapped_layers = []
        for i, gs_l in enumerate(gs_layers):
            # Filtering
            if workspace and gs_l.workspace != workspace:
                continue

            mapped_layers.append(
                {
                    "id": i + 1,  # Dummy ID
                    "layer_name": gs_l.name,
                    "title": gs_l.title,
                    "description": gs_l.abstract,
                    "workspace": gs_l.workspace,
                    "store_name": gs_l.store,
                    "srs": gs_l.srs,
                    "layer_type": "vector",  # Assumption or need better mapping
                    "geometry_type": "polygon",  # Assumption
                    "is_published": True,
                    "is_public": True,  # Assumption
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }
            )

        # Apply pagination
        total = len(mapped_layers)
        start = skip
        end = skip + limit
        paged_layers = mapped_layers[start:end]

        return LayerListResponse(
            layers=paged_layers, total=total, skip=skip, limit=limit
        )

    except Exception as e:
        import traceback

        traceback.print_exc()
        logger.error(f"Failed to fetch layers from GeoServer: {e}")
        raise HTTPException(status_code=500, detail=f"GeoServer Proxy Error: {str(e)}")


@router.get("/layers/{layer_name}", response_model=GeoLayerResponse)
async def get_geo_layer(layer_name: str, db: Session = Depends(get_db)):
    """Get a specific geospatial layer."""
    db_service = DatabaseService(db)
    return db_service.get_geo_layer(layer_name)


@router.put(
    "/layers/{layer_name}",
    response_model=GeoLayerResponse,
    dependencies=[Depends(has_role("admin"))],
)
async def update_geo_layer(
    layer_name: str, layer_update: GeoLayerUpdate, db: Session = Depends(get_db)
):
    """Update a geospatial layer."""
    db_service = DatabaseService(db)
    return db_service.update_geo_layer(layer_name, layer_update)


@router.delete(
    "/layers/{layer_name}", status_code=204, dependencies=[Depends(has_role("admin"))]
)
async def delete_geo_layer(layer_name: str, db: Session = Depends(get_db)):
    """Delete a geospatial layer."""
    db_service = DatabaseService(db)
    db_service.delete_geo_layer(layer_name)


@router.post("/features", response_model=GeoFeatureResponse, status_code=201)
async def create_geo_feature(
    feature: GeoFeatureCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Create a new geospatial feature."""
    db_service = DatabaseService(db)
    return db_service.create_geo_feature(feature)


@router.get("/features", response_model=FeatureListResponse)
async def get_geo_features(
    layer_name: str = Query(..., description="Layer name"),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records"),
    feature_type: Optional[str] = Query(None, description="Filter by feature type"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    bbox: Optional[str] = Query(
        None, description="Bounding box (min_lon,min_lat,max_lon,max_lat)"
    ),
    db: Session = Depends(get_db),
):
    """Get geospatial features with filtering."""
    db_service = DatabaseService(db)
    features = db_service.get_geo_features(
        layer_name=layer_name,
        skip=skip,
        limit=limit,
        feature_type=feature_type,
        is_active=is_active,
        bbox=bbox,
    )

    return FeatureListResponse(
        features=features,
        total=len(features),  # Todo: implement count in service for pagination
        layer_name=layer_name,
        skip=skip,
        limit=limit,
    )


@router.get("/features/{feature_id}", response_model=GeoFeatureResponse)
async def get_geo_feature(
    feature_id: str,
    layer_name: str = Query(..., description="Layer name"),
    db: Session = Depends(get_db),
):
    """Get a specific geospatial feature."""
    db_service = DatabaseService(db)
    return db_service.get_geo_feature(feature_id, layer_name)


@router.put("/features/{feature_id}", response_model=GeoFeatureResponse)
async def update_geo_feature(
    feature_id: str,
    feature_update: GeoFeatureUpdate,
    layer_name: str = Query(..., description="Layer name"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Update a geospatial feature."""
    db_service = DatabaseService(db)
    return db_service.update_geo_feature(feature_id, layer_name, feature_update)


@router.delete("/features/{feature_id}", status_code=204)
async def delete_geo_feature(
    feature_id: str,
    layer_name: str = Query(..., description="Layer name"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Delete a geospatial feature."""
    db_service = DatabaseService(db)
    db_service.delete_geo_feature(feature_id, layer_name)


@router.post("/spatial-query", response_model=SpatialQueryResponse)
async def spatial_query(query: SpatialQuery, db: Session = Depends(get_db)):
    """Perform spatial query on geospatial features."""
    try:
        # Note: You'll need to implement spatial_query in DatabaseService
        raise HTTPException(status_code=501, detail="Spatial query not yet implemented")
    except Exception as e:
        logger.error(f"Failed to perform spatial query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/geoserver/publish", status_code=201, dependencies=[Depends(has_role("admin"))]
)
async def publish_layer_to_geoserver(
    request: LayerPublishRequest, db: Session = Depends(get_db)
):
    """Publish a layer to GeoServer."""
    geoserver_service = GeoServerService()

    if not geoserver_service.test_connection():
        raise HTTPException(status_code=503, detail="Cannot connect to GeoServer")

    geoserver_service.create_workspace(request.workspace)

    success = geoserver_service.publish_layer(request)

    if success:
        return {"message": f"Layer {request.layer_name} published successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to publish layer")


@router.delete(
    "/geoserver/unpublish", status_code=204, dependencies=[Depends(has_role("admin"))]
)
async def unpublish_layer_from_geoserver(
    request: LayerUnpublishRequest, db: Session = Depends(get_db)
):
    """Unpublish a layer from GeoServer."""
    try:
        geoserver_service = GeoServerService()
        success = geoserver_service.unpublish_layer(
            request.layer_name, request.workspace
        )

        if success:
            return {"message": f"Layer {request.layer_name} unpublished successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to unpublish layer")
    except Exception as e:
        logger.error(f"Failed to unpublish layer from GeoServer: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geoserver/layers")
async def get_geoserver_layers(
    workspace: Optional[str] = Query(None, description="Filter by workspace")
):
    """Get layers from GeoServer."""
    try:
        geoserver_service = GeoServerService()

        if not geoserver_service.test_connection():
            raise HTTPException(status_code=503, detail="Cannot connect to GeoServer")

        layers = geoserver_service.get_layers(workspace)
        return {"layers": layers, "total": len(layers)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get GeoServer layers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geoserver/layers/{layer_name}")
async def get_geoserver_layer_info(
    layer_name: str,
    workspace: Optional[str] = Query(None, description="Workspace name"),
):
    """Get layer information from GeoServer."""
    try:
        geoserver_service = GeoServerService()

        if not geoserver_service.test_connection():
            raise HTTPException(status_code=503, detail="Cannot connect to GeoServer")

        layer_info = geoserver_service.get_layer_info(layer_name, workspace)
        if not layer_info:
            raise HTTPException(status_code=404, detail="Layer not found in GeoServer")

        return layer_info
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get GeoServer layer info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geoserver/layers/{layer_name}/capabilities")
async def get_layer_capabilities(
    layer_name: str,
    workspace: Optional[str] = Query(None, description="Workspace name"),
):
    """Get layer capabilities (WMS/WFS)."""
    try:
        geoserver_service = GeoServerService()

        if not geoserver_service.test_connection():
            raise HTTPException(status_code=503, detail="Cannot connect to GeoServer")

        capabilities = geoserver_service.get_layer_capabilities(layer_name, workspace)
        return capabilities
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get layer capabilities: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geoserver/layers/{layer_name}/wms-url")
async def get_wms_url(
    layer_name: str,
    workspace: Optional[str] = Query(None, description="Workspace name"),
    bbox: Optional[str] = Query(
        None, description="Bounding box (min_lon,min_lat,max_lon,max_lat)"
    ),
    width: int = Query(256, description="Image width"),
    height: int = Query(256, description="Image height"),
    srs: str = Query("EPSG:4326", description="Spatial reference system"),
    format: str = Query("image/png", description="Image format"),
):
    """Generate WMS URL for layer."""
    try:
        geoserver_service = GeoServerService()

        # Parse bbox if provided
        bbox_tuple = None
        if bbox:
            try:
                coords = [float(x) for x in bbox.split(",")]
                if len(coords) == 4:
                    bbox_tuple = tuple(coords)
            except ValueError:
                raise HTTPException(status_code=400, detail="Invalid bbox format")

        wms_url = geoserver_service.generate_wms_url(
            layer_name=layer_name,
            workspace=workspace,
            bbox=bbox_tuple,
            width=width,
            height=height,
            srs=srs,
            format=format,
        )

        return {"wms_url": wms_url}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to generate WMS URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geoserver/layers/{layer_name}/wfs-url")
async def get_wfs_url(
    layer_name: str,
    workspace: Optional[str] = Query(None, description="Workspace name"),
    output_format: str = Query("application/json", description="Output format"),
):
    """Generate WFS URL for layer."""
    try:
        geoserver_service = GeoServerService()

        wfs_url = geoserver_service.generate_wfs_url(
            layer_name=layer_name, workspace=workspace, output_format=output_format
        )

        return {"wfs_url": wfs_url}
    except Exception as e:
        logger.error(f"Failed to generate WFS URL: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/geoserver/layers/{layer_name}/geojson")
async def get_layer_geojson(
    layer_name: str,
    workspace: Optional[str] = Query(None, description="Workspace name"),
):
    """Get layer features as GeoJSON directly from GeoServer."""
    try:
        geoserver_service = GeoServerService()

        if not geoserver_service.test_connection():
            raise HTTPException(status_code=503, detail="Cannot connect to GeoServer")

        features = geoserver_service.get_wfs_features(
            layer_name=layer_name, workspace=workspace
        )
        return features
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get layer GeoJSON: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/layers/{layer_name}/sensors")
async def get_sensors_in_layer(
    layer_name: str,
    db: Session = Depends(get_db),
):
    """Get sensors (Things) within the specified layer's geometry."""
    try:
        db_service = DatabaseService(db)
        sensors = db_service.get_sensors_in_layer(layer_name)
        return sensors
    except Exception as e:
        logger.error(f"Failed to get sensors in layer {layer_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/layers/{layer_name}/bbox")
async def get_layer_bbox(
    layer_name: str,
    db: Session = Depends(get_db),
):
    """Get the bounding box of a layer."""
    try:
        db_service = DatabaseService(db)
        bbox = db_service.get_layer_bbox(layer_name)
        if not bbox:
            raise HTTPException(status_code=404, detail="BBox not found or layer empty")
        return {"bbox": bbox}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get bbox for layer {layer_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
