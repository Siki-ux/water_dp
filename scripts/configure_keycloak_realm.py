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

# Enable Registration
data["registrationAllowed"] = True
print("Enabled User Registration.")

# Ensure roles section exists and has "admin" role
if "roles" not in data:
    data["roles"] = {"realm": []}
if "realm" not in data["roles"]:
    data["roles"]["realm"] = []

admin_role_found = False
for role in data["roles"]["realm"]:
    if role.get("name") == "admin":
        admin_role_found = True
        break

if not admin_role_found:
    data["roles"]["realm"].append(
        {
            "name": "admin",
            "description": "Administrator role with full API access",
            "composite": False,
            "clientRole": False,
            "containerId": data.get("id", "35ec6dcb-8b6b-4581-b21d-ea6aae80a789"),
        }
    )
    print("Added 'admin' role to realm.")

# Define users to add
users = [
    {
        "username": "frontendbus",
        "enabled": True,
        "emailVerified": True,
        "email": "frontendbus@water-dp.local",
        "requiredActions": [],
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
        "email": "siki@water-dp.local",
        "requiredActions": [],
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
    {
        "username": "admin-siki",
        "enabled": True,
        "emailVerified": True,
        "email": "admin-siki@water-dp.local",
        "requiredActions": [],
        "firstName": "Admin",
        "lastName": "Siki",
        "credentials": [
            {
                "type": "password",
                "value": os.getenv("SEED_USER_ADMIN_SIKI_PASS", "admin-password"),
                "temporary": False,
            }
        ],
        "realmRoles": ["default-roles-timeio", "admin"],
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
        public_hostname = os.getenv("PUBLIC_HOSTNAME")
        base_uris = [
            "http://localhost:8000/*",  # FastAPI
            "http://localhost:8080/*",  # GeoServer
            "http://localhost:8082/*",  # Thing Management
            "http://localhost:3000/*",  # Grafana
            "http://localhost:8081/*",  # Keycloak
        ]
        if public_hostname:
            base_uris.extend([
                f"http://{public_hostname}/*",
                f"http://{public_hostname}:8000/*",
                f"http://{public_hostname}:8081/*",
                f"http://{public_hostname}:8082/*",
            ])
        
        client["redirectUris"] = base_uris
        client["webOrigins"] = ["+"]  # Allow origins matching redirect URIs
        print(f"Updated existing timeIO-client configuration with {len(client['redirectUris'])} URIs.")

if not client_found:
    print("timeIO-client not found. Creating it...")
    public_hostname = os.getenv("PUBLIC_HOSTNAME")
    base_uris = [
        "http://localhost:8000/*",
        "http://localhost:8080/*",
        "http://localhost:8082/*",
        "http://localhost:3000/*",
        "http://localhost:8081/*",
    ]
    if public_hostname:
        base_uris.extend([
            f"http://{public_hostname}/*",
            f"http://{public_hostname}:8000/*",
            f"http://{public_hostname}:8081/*",
            f"http://{public_hostname}:8082/*",
        ])
    
    new_client = {
        "clientId": "timeIO-client",
        "enabled": True,
        "publicClient": True,
        "standardFlowEnabled": True,
        "directAccessGrantsEnabled": True,
        "redirectUris": base_uris,
        "webOrigins": ["+"],
    }

# Define the mapper
mapper = {
    "name": "eduperson-entitlement-mapper",
    "protocol": "openid-connect",
    "protocolMapper": "oidc-hardcoded-claim-mapper",
    "consentRequired": False,
    "config": {
        "claim.name": "eduperson_entitlement",
        "claim.value": "[\"urn:geant:params:group:UFZ-TSM:MyProject\"]",
        "jsonType.label": "JSON",
        "id.token.claim": "true",
        "access.token.claim": "true",
        "userinfo.token.claim": "true"
    }
}

# Helper function to add mapper if missing and remove conflicting ones
def ensure_mapper(client_data):
    if "protocolMappers" not in client_data:
        client_data["protocolMappers"] = []
    
    # Remove conflicting mapper (underscore)
    client_data["protocolMappers"] = [
        m for m in client_data["protocolMappers"] 
        if m.get("name") != "eduperson_entitlement"
    ]
    
    mapper_exists = False
    for m in client_data["protocolMappers"]:
        if m.get("name") == mapper["name"]:
            mapper_exists = True
            break
    
    if not mapper_exists:
        client_data["protocolMappers"].append(mapper)
        print(f"Added {mapper['name']} to timeIO-client.")

if client_found:
    # We need to find the client again to modify it
    for client in data["clients"]:
        if client.get("clientId") == "timeIO-client":
            ensure_mapper(client)
            break
else:
    # Apply to new_client
    ensure_mapper(new_client)
    data["clients"].append(new_client)
    print("Created timeIO-client configuration.")

# Write back as UTF-8
with open(file_path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

print(f"Successfully updated {file_path} with users and UTF-8 encoding.")
