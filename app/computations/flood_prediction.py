import random
import time


def run(params: dict):
    """
    Example computation script that simulates a flood prediction model.
    """
    print(f"Starting flood prediction with params: {params}")

    # Simulate heavy computation
    time.sleep(5)

    # Simulate fetching data (mock)
    location_id = params.get("location_id", 1)

    # Dummy calculation
    risk_score = random.random() * 100
    is_flood_likely = risk_score > 80

    result = {
        "location_id": location_id,
        "risk_score": round(risk_score, 2),
        "prediction": "FLOOD" if is_flood_likely else "NORMAL",
        "timestamp": time.time(),
    }

    print(f"Computation finished: {result}")
    return result
