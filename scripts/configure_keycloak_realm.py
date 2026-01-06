import json
import os

file_path = "keycloak/import/timeio-realm.json"

# Read the file (handling PowerShell's UTF-16LE or default encoding)
try:
    with open(file_path, "r", encoding="utf-16") as f:
        data = json.load(f)
except Exception:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading file: {e}")
        exit(1)

# Define users to add
users = [
    {
        "username": "frontendbus",
        "enabled": True,
        "emailVerified": True,
        "firstName": "Frontend",
        "lastName": "Bus",
        "credentials": [
            {
                "type": "password",
                "value": os.getenv("SEED_USER_FRONTENDBUS_PASS", "frontendbus"),
                "temporary": False,
            }
        ],
        "realmRoles": ["default-roles-timeio"],
    },
    {
        "username": "siki",
        "enabled": True,
        "emailVerified": True,
        "firstName": "Siki",
        "lastName": "User",
        "credentials": [
            {
                "type": "password",
                "value": os.getenv("SEED_USER_SIKI_PASS", "password"),
                "temporary": False,
            }
        ],
        "realmRoles": ["default-roles-timeio"],
    },
]

# Add users to realm definition
data["users"] = users

# Ensure standard flow and direct access grants are enabled for timeIO-client if present
if "clients" not in data:
    data["clients"] = []

client_found = False
for client in data["clients"]:
    if client.get("clientId") == "timeIO-client":
        client_found = True
        client["standardFlowEnabled"] = True
        client["directAccessGrantsEnabled"] = True
        # Restrict to explicit localhost ports for security
        client["redirectUris"] = [
            "http://localhost:8000/*",  # FastAPI
            "http://localhost:8080/*",  # GeoServer
            "http://localhost:8082/*",  # Thing Management
            "http://localhost:3000/*",  # Grafana
            "http://localhost:8081/*",  # Keycloak
        ]
        client["webOrigins"] = ["+"]  # Allow origins matching redirect URIs
        print("Updated existing timeIO-client configuration.")

if not client_found:
    print("timeIO-client not found. Creating it...")
    new_client = {
        "clientId": "timeIO-client",
        "enabled": True,
        "publicClient": True,
        "standardFlowEnabled": True,
        "directAccessGrantsEnabled": True,
        "redirectUris": [
            "http://localhost:8000/*",
            "http://localhost:8080/*",
            "http://localhost:8082/*",
            "http://localhost:3000/*",
            "http://localhost:8081/*",
        ],
        "webOrigins": ["+"],
    }
    data["clients"].append(new_client)
    print("Created timeIO-client configuration.")

# Write back as UTF-8
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print(f"Successfully updated {file_path} with users and UTF-8 encoding.")
