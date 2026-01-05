"""
Geospatial API endpoints.
"""

import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

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


@router.post("/layers", response_model=GeoLayerResponse, status_code=201)
async def create_geo_layer(layer: GeoLayerCreate, db: Session = Depends(get_db)):
    """Create a new geospatial layer."""
    try:
        db_service = DatabaseService(db)
        return db_service.create_geo_layer(layer)
    except Exception as e:
        logger.error(f"Failed to create geo layer: {e}")
        raise HTTPException(status_code=400, detail=str(e))


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
    try:
        db_service = DatabaseService(db)
        layers = db_service.get_geo_layers(workspace=workspace, layer_type=layer_type)

        # Apply additional filters
        if is_published is not None:
            layers = [
                layer
                for layer in layers
                if layer.is_published == str(is_published).lower()
            ]
        if is_public is not None:
            layers = [
                layer for layer in layers if layer.is_public == str(is_public).lower()
            ]

        # Apply pagination
        total = len(layers)
        layers = layers[skip : skip + limit]

        return LayerListResponse(layers=layers, total=total, skip=skip, limit=limit)
    except Exception as e:
        logger.error(f"Failed to get geo layers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/layers/{layer_name}", response_model=GeoLayerResponse)
async def get_geo_layer(layer_name: str, db: Session = Depends(get_db)):
    """Get a specific geospatial layer."""
    try:
        db_service = DatabaseService(db)
        layer = db_service.get_geo_layer(layer_name)
        if not layer:
            raise HTTPException(
                status_code=404, detail=f"Geo layer {layer_name} not found"
            )
        return layer
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get geo layer {layer_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/layers/{layer_name}", response_model=GeoLayerResponse)
async def update_geo_layer(
    layer_name: str, layer_update: GeoLayerUpdate, db: Session = Depends(get_db)
):
    """Update a geospatial layer."""
    try:
        db_service = DatabaseService(db)
        layer = db_service.update_geo_layer(layer_name, layer_update)
        if not layer:
            raise HTTPException(
                status_code=404, detail=f"Geo layer {layer_name} not found"
            )
        return layer
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update geo layer {layer_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/layers/{layer_name}", status_code=204)
async def delete_geo_layer(layer_name: str, db: Session = Depends(get_db)):
    """Delete a geospatial layer."""
    try:
        db_service = DatabaseService(db)
        success = db_service.delete_geo_layer(layer_name)
        if not success:
            raise HTTPException(
                status_code=404, detail=f"Geo layer {layer_name} not found"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete geo layer {layer_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/features", response_model=GeoFeatureResponse, status_code=201)
async def create_geo_feature(feature: GeoFeatureCreate, db: Session = Depends(get_db)):
    """Create a new geospatial feature."""
    try:
        db_service = DatabaseService(db)
        return db_service.create_geo_feature(feature)
    except Exception as e:
        logger.error(f"Failed to create geo feature: {e}")
        raise HTTPException(status_code=400, detail=str(e))


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
    try:
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
    except Exception as e:
        logger.error(f"Failed to get geo features: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/features/{feature_id}", response_model=GeoFeatureResponse)
async def get_geo_feature(
    feature_id: str,
    layer_name: str = Query(..., description="Layer name"),
    db: Session = Depends(get_db),
):
    """Get a specific geospatial feature."""
    try:
        db_service = DatabaseService(db)
        feature = db_service.get_geo_feature(feature_id, layer_name)
        if not feature:
            raise HTTPException(
                status_code=404,
                detail=f"Geo feature {feature_id} not found in layer {layer_name}",
            )
        return feature
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get geo feature {feature_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/features/{feature_id}", response_model=GeoFeatureResponse)
async def update_geo_feature(
    feature_id: str,
    feature_update: GeoFeatureUpdate,
    layer_name: str = Query(..., description="Layer name"),
    db: Session = Depends(get_db),
):
    """Update a geospatial feature."""
    try:
        db_service = DatabaseService(db)
        feature = db_service.update_geo_feature(feature_id, layer_name, feature_update)
        if not feature:
            raise HTTPException(
                status_code=404,
                detail=f"Geo feature {feature_id} not found in layer {layer_name}",
            )
        return feature
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update geo feature {feature_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/features/{feature_id}", status_code=204)
async def delete_geo_feature(
    feature_id: str,
    layer_name: str = Query(..., description="Layer name"),
    db: Session = Depends(get_db),
):
    """Delete a geospatial feature."""
    try:
        db_service = DatabaseService(db)
        success = db_service.delete_geo_feature(feature_id, layer_name)
        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Geo feature {feature_id} not found in layer {layer_name}",
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete geo feature {feature_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/spatial-query", response_model=SpatialQueryResponse)
async def spatial_query(query: SpatialQuery, db: Session = Depends(get_db)):
    """Perform spatial query on geospatial features."""
    try:
        # Note: You'll need to implement spatial_query in DatabaseService
        raise HTTPException(status_code=501, detail="Spatial query not yet implemented")
    except Exception as e:
        logger.error(f"Failed to perform spatial query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/geoserver/publish", status_code=201)
async def publish_layer_to_geoserver(
    request: LayerPublishRequest, db: Session = Depends(get_db)
):
    """Publish a layer to GeoServer."""
    try:
        geoserver_service = GeoServerService()

        if not geoserver_service.test_connection():
            raise HTTPException(status_code=503, detail="Cannot connect to GeoServer")

        geoserver_service.create_workspace(request.workspace)

        success = geoserver_service.publish_layer(request)

        if success:
            return {"message": f"Layer {request.layer_name} published successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to publish layer")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to publish layer to GeoServer: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/geoserver/unpublish", status_code=204)
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
