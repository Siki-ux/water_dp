"""
Time series data processing and analysis service.
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import pytz
from sqlalchemy.orm import Session

from app.models.time_series import TimeSeriesData
from app.schemas.time_series import (
    AggregatedDataPoint,
    InterpolationRequest,
    TimeSeriesAggregation,
    TimeSeriesQuery,
    TimeSeriesStatistics,
)

logger = logging.getLogger(__name__)


class TimeSeriesService:
    """Service for time series data processing and analysis."""

    def __init__(self, db: Session):
        self.db = db

    def get_time_series_data(self, query: TimeSeriesQuery) -> List[TimeSeriesData]:
        """Get time series data with filtering."""
        try:
            db_query = self.db.query(TimeSeriesData).filter(
                TimeSeriesData.series_id == query.series_id
            )

            if query.start_time:
                db_query = db_query.filter(TimeSeriesData.timestamp >= query.start_time)
            if query.end_time:
                db_query = db_query.filter(TimeSeriesData.timestamp <= query.end_time)
            if query.quality_filter:
                db_query = db_query.filter(
                    TimeSeriesData.quality_flag == query.quality_filter
                )
            if not query.include_interpolated:
                db_query = db_query.filter(
                    TimeSeriesData.is_interpolated == False  # noqa: E712
                )
            if not query.include_aggregated:
                db_query = db_query.filter(
                    TimeSeriesData.is_aggregated == False  # noqa: E712
                )

            return db_query.order_by(TimeSeriesData.timestamp).limit(query.limit).all()

        except Exception as e:
            logger.error(f"Failed to get time series data: {e}")
            raise

    def aggregate_time_series(
        self, aggregation: TimeSeriesAggregation
    ) -> List[AggregatedDataPoint]:
        """Aggregate time series data."""
        try:
            # Get raw data
            query = TimeSeriesQuery(
                series_id=aggregation.series_id,
                start_time=aggregation.start_time,
                end_time=aggregation.end_time,
                limit=100000,  # Large limit for aggregation
            )
            data_points = self.get_time_series_data(query)

            if not data_points:
                return []

            # Convert to DataFrame
            df = pd.DataFrame(
                [
                    {
                        "timestamp": point.timestamp,
                        "value": point.value,
                        "quality_flag": point.quality_flag,
                    }
                    for point in data_points
                ]
            )

            # Set timezone
            if aggregation.time_zone != "UTC":
                df["timestamp"] = (
                    df["timestamp"]
                    .dt.tz_localize("UTC")
                    .dt.tz_convert(aggregation.time_zone)
                )

            # Set timestamp as index
            df.set_index("timestamp", inplace=True)

            # Resample based on interval
            interval_map = {
                "1min": "1min",
                "5min": "5min",
                "15min": "15min",
                "30min": "30min",
                "1hour": "1h",
                "6hour": "6h",
                "12hour": "12h",
                "1day": "1D",
                "1week": "1W",
                "1month": "1M",
                "1year": "1Y",
            }

            pandas_freq = interval_map.get(aggregation.aggregation_interval, "1h")

            # Apply aggregation method
            if aggregation.aggregation_method == "mean":
                aggregated = df["value"].resample(pandas_freq).mean()
                count = df["value"].resample(pandas_freq).count()
            elif aggregation.aggregation_method == "max":
                aggregated = df["value"].resample(pandas_freq).max()
                count = df["value"].resample(pandas_freq).count()
            elif aggregation.aggregation_method == "min":
                aggregated = df["value"].resample(pandas_freq).min()
                count = df["value"].resample(pandas_freq).count()
            elif aggregation.aggregation_method == "sum":
                aggregated = df["value"].resample(pandas_freq).sum()
                count = df["value"].resample(pandas_freq).count()
            elif aggregation.aggregation_method == "count":
                aggregated = df["value"].resample(pandas_freq).count()
                count = aggregated
            elif aggregation.aggregation_method == "std":
                aggregated = df["value"].resample(pandas_freq).std()
                count = df["value"].resample(pandas_freq).count()
            elif aggregation.aggregation_method == "median":
                aggregated = df["value"].resample(pandas_freq).median()
                count = df["value"].resample(pandas_freq).count()
            else:
                raise ValueError(
                    f"Unsupported aggregation method: {aggregation.aggregation_method}"
                )

            # Get quality flags for each period
            quality_flags = df["quality_flag"].resample(pandas_freq).apply(list)

            # Create aggregated data points
            aggregated_points = []
            for timestamp, value in aggregated.items():
                if pd.notna(value):
                    aggregated_points.append(
                        AggregatedDataPoint(
                            timestamp=timestamp,
                            value=float(value),
                            count=int(count.get(timestamp, 0)),
                            aggregation_method=aggregation.aggregation_method,
                            aggregation_interval=aggregation.aggregation_interval,
                            quality_flags=quality_flags.get(timestamp, []),
                            metadata={
                                "series_id": aggregation.series_id,
                                "time_zone": aggregation.time_zone,
                            },
                        )
                    )

            return aggregated_points

        except Exception as e:
            logger.error(f"Failed to aggregate time series: {e}")
            raise

    def interpolate_time_series(
        self, request: InterpolationRequest
    ) -> List[TimeSeriesData]:
        """Interpolate missing values in time series."""
        try:
            # Get existing data
            query = TimeSeriesQuery(
                series_id=request.series_id,
                start_time=request.start_time,
                end_time=request.end_time,
                limit=100000,
            )
            data_points = self.get_time_series_data(query)

            if not data_points:
                return []

            # Convert to DataFrame
            df = pd.DataFrame(
                [
                    {
                        "timestamp": point.timestamp,
                        "value": point.value,
                        "quality_flag": point.quality_flag,
                    }
                    for point in data_points
                ]
            )

            df.set_index("timestamp", inplace=True)

            # Create target time index
            # Create target time index
            interval_map = {
                "1min": "1min",
                "5min": "5min",
                "15min": "15min",
                "30min": "30min",
                "1hour": "1h",
                "6hour": "6h",
                "12hour": "12h",
                "1day": "1D",
            }

            pandas_freq = interval_map.get(request.interval, "1h")
            target_index = pd.date_range(
                start=request.start_time, end=request.end_time, freq=pandas_freq
            )

            # Reindex and interpolate
            df_reindexed = df.reindex(target_index)

            if request.method == "linear":
                interpolated_values = df_reindexed["value"].interpolate(method="linear")
            elif request.method == "spline":
                interpolated_values = df_reindexed["value"].interpolate(
                    method="spline", order=3
                )
            elif request.method == "polynomial":
                interpolated_values = df_reindexed["value"].interpolate(
                    method="polynomial", order=2
                )
            else:
                interpolated_values = df_reindexed["value"].interpolate(method="linear")

            # Identify interpolated points
            original_timestamps = set(df.index)
            interpolated_points = []

            for timestamp, value in interpolated_values.items():
                if pd.notna(value):
                    is_interpolated = timestamp not in original_timestamps

                    interpolated_points.append(
                        TimeSeriesData(
                            series_id=request.series_id,
                            timestamp=timestamp,
                            value=float(value),
                            quality_flag="interpolated" if is_interpolated else "good",
                            is_interpolated=is_interpolated,
                            is_aggregated=False,
                            properties={
                                "interpolation_method": request.method,
                                "original_data_count": len(data_points),
                            },
                        )
                    )

            return interpolated_points

        except Exception as e:
            logger.error(f"Failed to interpolate time series: {e}")
            raise

    def detect_anomalies(
        self,
        series_id: str,
        start_time: datetime,
        end_time: datetime,
        method: str = "statistical",
        threshold: float = 3.0,
    ) -> List[Dict[str, Any]]:
        """Detect anomalies in time series data."""
        try:
            query = TimeSeriesQuery(
                series_id=series_id,
                start_time=start_time,
                end_time=end_time,
                limit=100000,
            )
            data_points = self.get_time_series_data(query)

            if len(data_points) < 10:
                return []

            # Convert to DataFrame
            df = pd.DataFrame(
                [
                    {"timestamp": point.timestamp, "value": point.value}
                    for point in data_points
                ]
            )

            df.set_index("timestamp", inplace=True)

            anomalies = []

            if method == "statistical":
                # Z-score method
                mean_val = df["value"].mean()
                std_val = df["value"].std()
                z_scores = np.abs((df["value"] - mean_val) / std_val)

                anomaly_indices = z_scores > threshold

                for timestamp, is_anomaly in anomaly_indices.items():
                    if is_anomaly:
                        anomalies.append(
                            {
                                "timestamp": timestamp,
                                "value": float(df.loc[timestamp, "value"]),
                                "z_score": float(z_scores[timestamp]),
                                "method": method,
                                "threshold": threshold,
                            }
                        )

            elif method == "iqr":
                # Interquartile Range method
                Q1 = df["value"].quantile(0.25)
                Q3 = df["value"].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR

                anomaly_mask = (df["value"] < lower_bound) | (df["value"] > upper_bound)

                for timestamp, is_anomaly in anomaly_mask.items():
                    if is_anomaly:
                        anomalies.append(
                            {
                                "timestamp": timestamp,
                                "value": float(df.loc[timestamp, "value"]),
                                "lower_bound": float(lower_bound),
                                "upper_bound": float(upper_bound),
                                "method": method,
                            }
                        )

            return anomalies

        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            raise

    def calculate_statistics(
        self, series_id: str, start_time: datetime, end_time: datetime
    ) -> TimeSeriesStatistics:
        """Calculate comprehensive statistics for time series."""
        try:
            query = TimeSeriesQuery(
                series_id=series_id,
                start_time=start_time,
                end_time=end_time,
                limit=100000,
            )
            data_points = self.get_time_series_data(query)

            if not data_points:
                return TimeSeriesStatistics(
                    series_id=series_id,
                    time_range={"start": start_time, "end": end_time},
                    total_points=0,
                    statistics={},
                    quality_summary={},
                    gaps=[],
                    metadata={},
                )

            # Convert to DataFrame
            df = pd.DataFrame(
                [
                    {
                        "timestamp": point.timestamp,
                        "value": point.value,
                        "quality_flag": point.quality_flag,
                    }
                    for point in data_points
                ]
            )

            # Basic statistics
            stats = {
                "count": len(df),
                "mean": float(df["value"].mean()),
                "median": float(df["value"].median()),
                "std": float(df["value"].std()),
                "min": float(df["value"].min()),
                "max": float(df["value"].max()),
                "range": float(df["value"].max() - df["value"].min()),
                "q25": float(df["value"].quantile(0.25)),
                "q75": float(df["value"].quantile(0.75)),
                "iqr": float(df["value"].quantile(0.75) - df["value"].quantile(0.25)),
            }

            # Quality summary
            quality_summary = df["quality_flag"].value_counts().to_dict()

            # Detect gaps
            df_sorted = df.sort_values("timestamp")
            gaps = []

            for i in range(1, len(df_sorted)):
                time_diff = (
                    df_sorted.iloc[i]["timestamp"] - df_sorted.iloc[i - 1]["timestamp"]
                )
                if time_diff > timedelta(hours=24):  # Gap larger than 24 hours
                    gaps.append(
                        {
                            "start": df_sorted.iloc[i - 1]["timestamp"],
                            "end": df_sorted.iloc[i]["timestamp"],
                            "duration_hours": time_diff.total_seconds() / 3600,
                        }
                    )

            return TimeSeriesStatistics(
                series_id=series_id,
                time_range={"start": start_time, "end": end_time},
                total_points=len(df),
                statistics=stats,
                quality_summary=quality_summary,
                gaps=gaps,
                metadata={
                    "analysis_timestamp": datetime.now(pytz.UTC),
                    "data_quality_score": self._calculate_quality_score(
                        quality_summary
                    ),
                },
            )

        except Exception as e:
            logger.error(f"Failed to calculate statistics: {e}")
            raise

    def _calculate_quality_score(self, quality_summary: Dict[str, int]) -> float:
        """Calculate data quality score (0-1)."""
        total = sum(quality_summary.values())
        if total == 0:
            return 0.0

        good_count = quality_summary.get("good", 0)
        questionable_count = quality_summary.get("questionable", 0)

        # Weight good data as 1.0, questionable as 0.5
        score = (good_count * 1.0 + questionable_count * 0.5) / total
        return min(1.0, max(0.0, score))

    def export_time_series(
        self,
        series_id: str,
        start_time: datetime,
        end_time: datetime,
        format: str = "csv",
    ) -> str:
        """Export time series data in specified format."""
        try:
            query = TimeSeriesQuery(
                series_id=series_id,
                start_time=start_time,
                end_time=end_time,
                limit=100000,
            )
            data_points = self.get_time_series_data(query)

            # Convert to DataFrame
            df = pd.DataFrame(
                [
                    {
                        "timestamp": point.timestamp,
                        "value": point.value,
                        "quality_flag": point.quality_flag,
                        "uncertainty": point.uncertainty,
                        "is_interpolated": point.is_interpolated,
                        "is_aggregated": point.is_aggregated,
                    }
                    for point in data_points
                ]
            )

            if format.lower() == "csv":
                return df.to_csv(index=False)
            elif format.lower() == "json":
                return df.to_json(orient="records", date_format="iso")
            elif format.lower() == "excel":
                return df.to_excel(index=False)
            else:
                raise ValueError(f"Unsupported export format: {format}")

        except Exception as e:
            logger.error(f"Failed to export time series: {e}")
            raise
