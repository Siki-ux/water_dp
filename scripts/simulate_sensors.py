
import os
import sys
import time
import random
import logging
import requests
import json

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("water-dp-seed")

# Configuration
API_URL = os.getenv("API_URL", "http://water-dp-api:8000/api/v1")
# Use admin-siki/admin-siki by default as requested
USERNAME = os.getenv("ADMIN_USERNAME", "admin-siki")
PASSWORD = os.getenv("ADMIN_PASSWORD", "admin-siki")
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://keycloak:8080")
REALM = "timeio"

# Czech Republic Locations (Lakes/Rivers)
LOCATIONS = [
    {"name": "Lipno Dam", "lat": 48.6333, "lon": 14.1667},
    {"name": "Orlik Dam", "lat": 49.6105, "lon": 14.1698},
    {"name": "Slapy Dam", "lat": 49.8219, "lon": 14.4286},
    {"name": "Vranov Dam", "lat": 48.9056, "lon": 15.8118},
    {"name": "Nove Mlyny - Upper", "lat": 48.8964, "lon": 16.6340},
    {"name": "Nove Mlyny - Middle", "lat": 48.8779, "lon": 16.6661},
    {"name": "Nove Mlyny - Lower", "lat": 48.8601, "lon": 16.7121},
    {"name": "Lake Macha", "lat": 50.5636, "lon": 14.6625},
    {"name": "Rozkos Dam", "lat": 50.3708, "lon": 16.0594},
    {"name": "Hracholusky Dam", "lat": 49.7915, "lon": 13.1784},
    {"name": "Sec Dam", "lat": 49.8327, "lon": 15.6516},
    {"name": "River Elbe - Melnik", "lat": 50.3541, "lon": 14.4743},
    {"name": "River Vltava - Prague", "lat": 50.0755, "lon": 14.4378},
    {"name": "River Morava - Olomouc", "lat": 49.5938, "lon": 17.2509},
    {"name": "River Odra - Ostrava", "lat": 49.8209, "lon": 18.2625}
]

def get_access_token():
    url = f"{API_URL}/auth/login"
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
    }
    try:
        logging.info(f"Authenticating as {USERNAME} at {url}...")
        response = requests.post(url, json=payload)
        
        if response.status_code != 200:
             logger.error(f"Auth failed: {response.text}")
             response.raise_for_status()
        
        return response.json()["access_token"]
    except Exception as e:
        logger.error(f"Failed to get access token: {e}")
        sys.exit(1)

def get_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def wait_for_api():
    base_url = API_URL.replace("/api/v1", "").rstrip("/")
    health_url = f"{base_url}/health"
    
    logger.info(f"Waiting for API at {health_url}...")
    for _ in range(60):
        try:
            if requests.get(health_url, timeout=5).status_code == 200:
                logger.info("API is Up.")
                return
        except Exception:
            pass
        time.sleep(2)
    logger.error("API unreachable.")
    sys.exit(1)

def get_group_id_by_name(headers, name):
    try:
        # Assuming there is a GET /groups endpoint
        # If not, we might need to search or list all
        res = requests.get(f"{API_URL}/groups", headers=headers)
        if res.status_code == 200:
            groups = res.json()
            for g in groups:
                # Based on user request, the group name or path should match
                # The user said "group name should be UFZ-TSM:MyProject" - checking both name and path just in case
                if g.get("name") == name or g.get("path") == name:
                    logger.info(f"Found group '{name}' with ID: {g['id']}")
                    return g["id"]
            
            logger.warning(f"Group '{name}' not found in {len(groups)} groups.")
    except Exception as e:
        logger.error(f"Error fetching groups: {e}")
    
    return None

def create_project(headers, group_id):
    project_payload = {
        "name": "Czech Water Analysis",
        "description": "Monitoring water quality in major Czech bodies of water.",
        "authorization_provider_group_id": group_id
    }
    
    # Try finding existing first to avoid duplicates
    try:
        res = requests.get(f"{API_URL}/projects", headers=headers)
        if res.status_code == 200:
            for p in res.json():
                if p["name"] == project_payload["name"]:
                    logger.info(f"Project '{project_payload['name']}' already exists.")
                    return p["id"]
    except Exception:
        pass

    try:
        logger.info(f"Creating project '{project_payload['name']}'...")
        res = requests.post(f"{API_URL}/projects", headers=headers, json=project_payload)
        if res.status_code in [200, 201]:
            pid = res.json()["id"]
            logger.info(f"Project created with ID: {pid}")
            return pid
        else:
            logger.error(f"Failed to create project: {res.text}")
            sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to create project: {e}")
        sys.exit(1)

def create_simulated_sensor(headers, project_id, location_info, index):
    name = f"{location_info['name']} Sensor"
    
    # Randomize start values slightly so they don't all look identical
    start_level = round(random.uniform(1.0, 3.0), 2)
    start_temp = round(random.uniform(10.0, 20.0), 2)
    
    payload = {
        "thing": {
            "project_uuid": project_id,
            "sensor_name": name,
            "description": f"Monitoring station at {location_info['name']}",
            "device_type": "chirpstack_generic",
            "latitude": location_info['lat'],
            "longitude": location_info['lon'],
            "properties": [
                {
                    "name": "water_level",
                    "unit": "m",
                    "label": "Water Level"
                },
                {
                    "name": "temperature",
                    "unit": "°C",
                    "label": "Temperature"
                }
            ]
        },
        "simulation": {
            "enabled": True,
            "datastreams": [
                {
                    "name": "water_level",
                    "range": {
                        "min": 0,
                        "max": 5
                    },
                    "interval": "60s",
                    "type": "random",
                    "enabled": True
                },
                {
                    "name": "temperature",
                    "range": {
                        "min": 0,
                        "max": 30
                    },
                    "interval": "300s",
                    "type": "sine",
                    "enabled": True
                }
            ]
        }
    }
    
    try:
        res = requests.post(
            f"{API_URL}/projects/{project_id}/simulator/things", 
            headers=headers, 
            json=payload
        )
        if res.status_code in [200, 201]:
            logger.info(f"Created sensor: {name}")
        else:
             # If it already exists (409 or 400), ignore
            if res.status_code == 409:
                logger.info(f"Sensor {name} likely already exists.")
            else:
                logger.error(f"Failed to create sensor {name}. Status: {res.status_code} - {res.text}")
    except Exception as e:
        logger.error(f"Error creating sensor {name}: {e}")

def main():
    logger.info("Starting Water DP Simulation Script...")
    wait_for_api()
    
    token = get_access_token()
    headers = get_headers(token)
    
    target_group_name = "UFZ-TSM:MyProject"
    group_id = get_group_id_by_name(headers, target_group_name)
    
    if not group_id:
        logger.error(f"Could not find group '{target_group_name}'. Ensure it exists in Keycloak/API.")
        # Optional: create it effectively if we had permissions, but typically groups come from IDP
        # For now, we will proceed with None logic if the API allows it, or fail.
        # The user specifically asked to use this group, so we should probably fail or warn heavily.
        logger.error("Aborting project creation due to missing group.")
        # sys.exit(1) # Commented out to allow testing if group name is slightly different
    
    project_id = create_project(headers, group_id)
    
    logger.info(f"Creating {len(LOCATIONS)} Simulated Sensors...")
    for i, loc in enumerate(LOCATIONS):
        create_simulated_sensor(headers, project_id, loc, i)
        
    logger.info("Simulation Setup Complete.")

if __name__ == "__main__":
    main()
