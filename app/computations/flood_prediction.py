"""
Flood Prediction Model (Demo)
This script simulates a flood risk calculation based on water level.
"""

import random


def run(ctx):
    """
    Main entry point for computation.
    :param ctx: ComputationContext object with helper methods (get_sensor_data, alert, params)
    """
    # 1. Get Inputs (from params or live sensor)
    location_id = ctx.params.get("location_id")
    water_level = ctx.params.get("water_level")

    # Demo Fallback: If no location_id, try a likely existing one (e.g. '1')
    if not location_id and not water_level:
        location_id = "1"

    # If water_level not provided but location is, try to fetch from sensor
    sensor_fetched = False
    if water_level is None and location_id:
        # returns list of data points
        try:
            data = ctx.get_sensor_data(location_id, limit=1)
            if data:
                val = data[0].get("value")
                if val is not None:
                    water_level = float(val)
                    sensor_fetched = True
        except Exception:
            pass

    # Fallback to Simulation if still None
    if water_level is None:
        # Simulate a value for demo purposes
        water_level = round(random.uniform(50.0, 160.0), 2)

    # 2. Simulate Processing
    risk_score = 0.0
    if water_level > 150:
        risk_score = 80.0
    elif water_level > 100:
        risk_score = 40.0
    else:
        risk_score = 10.0

    prediction = "FLOOD" if risk_score > 50 else "NORMAL"

    # 3. Active Alerting (Script-Driven)
    if risk_score > 75:
        ctx.alert(
            message=f"CRITICAL FLOOD RISK detected at {location_id or 'Simulation'}",
            details={"water_level": water_level, "risk_score": risk_score},
            severity="critical",
        )
    elif risk_score > 50:
        ctx.alert(
            message=f"High Flood Risk at {location_id or 'Simulation'}",
            details={"water_level": water_level, "risk_score": risk_score},
            severity="warning",
        )

    # 4. Return Results
    result = {
        "status": "success",
        "location_id": location_id,
        "source": "sensor" if sensor_fetched else "simulation",
        "input_level": water_level,
        "risk_score": min(risk_score, 100),
        "prediction": prediction,
        "alert_triggered": risk_score > 50,
    }

    return result
