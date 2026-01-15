"""
Flood Prediction Model (Demo)
This script simulates a flood risk calculation based on water level.
"""


def run(params):
    """
    Main entry point for computation.
    params: dict containing input parameters
    """
    water_level = params.get("water_level", 0.0)

    # Simulate processing
    risk_score = 0.0
    if water_level > 150:
        risk_score = (water_level - 150) * 2.0

    result = {
        "status": "success",
        "location_id": params.get("location_id"),
        "input_level": water_level,
        "risk_score": min(risk_score, 100),
        "prediction": "FLOOD" if risk_score > 50 else "NORMAL",
        "alert_triggered": risk_score > 50,
    }

    return result
