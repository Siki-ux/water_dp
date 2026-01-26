import os
import sys

import requests
from keycloak import KeycloakAdmin, KeycloakError

# Configuration
KEYCLOAK_URL = os.getenv("KEYCLOAK_URL", "http://localhost:8081/keycloak").rstrip("/")
ADMIN_USER = os.getenv("KEYCLOAK_ADMIN_USERNAME", "keycloak")
ADMIN_PASSWORD = os.getenv("KEYCLOAK_ADMIN_PASSWORD", "keycloak")
KEYCLOAK_REALM = "timeio"
CLIENT_ID_NAME = "timeIO-client"


def get_admin_token():
    """Authenticate as admin to get an access token."""
    url = f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token"
    payload = {
        "grant_type": "password",
        "client_id": "admin-cli",
        "username": ADMIN_USER,
        "password": ADMIN_PASSWORD,
    }
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        print(f"Error getting admin token: {e}")
        print(f"Response: {response.text if 'response' in locals() else 'No response'}")
        sys.exit(1)


def get_client_id(token, client_name):
    """Get the internal UUID of the client by its clientId name."""
    url = f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"clientId": client_name}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    clients = response.json()

    if not clients:
        print(f"Client '{client_name}' not found.")
        return None

    return clients[0]["id"]


# ----------------------------------------------------------------------

# ----------------------------------------------------------------------
# Helper Functions
# ----------------------------------------------------------------------


def find_user(token, realm, username):
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"username": username, "exact": True}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    users = response.json()
    if users:
        return users[0]
    return None


def find_group(token, realm, name):
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/groups"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"search": name}  # Fuzzy search
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    groups = response.json()
    # Filter for exact match
    for g in groups:
        if g["name"] == name:
            return g
    return None


def find_child_group(token, realm, parent_id, child_name):
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/groups/{parent_id}/children"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    children = response.json()
    for c in children:
        if c["name"] == child_name:
            return c
    return None


def create_group(token, realm, name, parent_id=None):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"name": name}

    if parent_id:
        url = f"{KEYCLOAK_URL}/admin/realms/{realm}/groups/{parent_id}/children"
    else:
        url = f"{KEYCLOAK_URL}/admin/realms/{realm}/groups"

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201:
        print(f"Created group '{name}' (Parent: {parent_id})")
        if parent_id:
            return find_child_group(token, realm, parent_id, name)["id"]
        else:
            return find_group(token, realm, name)["id"]
    elif response.status_code == 409:
        print(f"Group '{name}' already exists.")
        if parent_id:
            return find_child_group(token, realm, parent_id, name)["id"]
        else:
            return find_group(token, realm, name)["id"]
    else:
        print(f"Failed to create group '{name}': {response.text}")
        return None


def join_group(token, realm, user_id, group_id):
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}/groups/{group_id}"
    headers = {"Authorization": f"Bearer {token}"}
    requests.put(url, headers=headers)
    print(f"Added user {user_id} to group {group_id}")


def delete_user(token, realm, user_id):
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}"
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.delete(url, headers=headers)
        response.raise_for_status()
        print(f"Deleted user {user_id} from {realm}")
    except Exception as e:
        print(f"Error deleting user: {e}")


def create_user(token, realm, username, password):
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {
        "username": username,
        "email": f"{username}@example.com",
        "enabled": True,
        "firstName": "Admin",
        "lastName": "Siki",
        "credentials": [{"value": password, "type": "password", "temporary": False}],
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        print(f"Created user {username} in {realm}")
        # Get ID
        user = find_user(token, realm, username)
        return user["id"]
    elif response.status_code == 409:
        print(f"User {username} already exists in {realm}")
        user = find_user(token, realm, username)
        return user["id"]
    else:
        print(f"Failed to create user: {response.text}")
        response.raise_for_status()


def assign_role(token, realm, user_id, role_name):
    # 1. Get Role
    url = f"{KEYCLOAK_URL}/admin/realms/{realm}/roles/{role_name}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 404:
        # Try to create it if missing? Or assume it should exist.
        print(f"Role {role_name} not found in {realm}. Creating...")
        create_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/roles"
        requests.post(create_url, headers=headers, json={"name": role_name})
        response = requests.get(url, headers=headers)

    response.raise_for_status()
    role_rep = response.json()

    # 2. Assign
    assign_url = (
        f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}/role-mappings/realm"
    )
    requests.post(assign_url, headers=headers, json=[role_rep])
    print(f"Assigned realm role {role_name} to user")


def assign_client_role(token, realm, user_id, client_client_id, role_name):
    # 1. Get Client UUID
    client_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/clients"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(
        client_url, headers=headers, params={"clientId": client_client_id}
    )
    response.raise_for_status()
    clients = response.json()
    if not clients:
        print(f"Client {client_client_id} not found")
        return
    client_uuid = clients[0]["id"]

    # 2. Get Role
    role_url = (
        f"{KEYCLOAK_URL}/admin/realms/{realm}/clients/{client_uuid}/roles/{role_name}"
    )
    response = requests.get(role_url, headers=headers)
    role_rep = response.json()

    # 3. Assign
    assign_url = f"{KEYCLOAK_URL}/admin/realms/{realm}/users/{user_id}/role-mappings/clients/{client_uuid}"
    requests.post(assign_url, headers=headers, json=[role_rep])
    print(f"Assigned client role {client_client_id}/{role_name} to user")


def enable_direct_access_grants(token, client_uuid):
    """Update the client to enable direct access grants."""
    url = f"{KEYCLOAK_URL}/admin/realms/{KEYCLOAK_REALM}/clients/{client_uuid}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Update only the specific field
    payload = {"directAccessGrantsEnabled": True}

    response = requests.put(url, headers=headers, json=payload)
    if response.status_code == 204:
        print(f"Successfully enabled Direct Access Grants for client {CLIENT_ID_NAME}")
    else:
        print(f"Failed to update client. Status: {response.status_code}")
        print(response.text)


if __name__ == "__main__":
    print(f"Connecting to Keycloak at {KEYCLOAK_URL}...")

    # Initialize KeycloakAdmin
    # Ensure server_url ends with a slash to prevent python-keycloak from stripping the path
    server_url_for_admin = (
        KEYCLOAK_URL + "/" if not KEYCLOAK_URL.endswith("/") else KEYCLOAK_URL
    )
    keycloak_admin = KeycloakAdmin(
        server_url=server_url_for_admin,
        username=ADMIN_USER,
        password=ADMIN_PASSWORD,
        realm_name="master",  # Admin operations are typically against the master realm
        verify=True,  # Set to False if you have SSL issues and want to ignore them
    )

    print("KeycloakAdmin initialized.")
    # Switch context to the target realm
    keycloak_admin.realm_name = KEYCLOAK_REALM
    print(f"Switched KeycloakAdmin context to realm: {keycloak_admin.realm_name}")

    # ----------------------------------------------------------------------
    # 1. Update SSL Requirement for Master and TimeIO Realms
    # ----------------------------------------------------------------------
    print("Updating 'master' realm sslRequired to 'NONE'...")
    try:
        keycloak_admin.update_realm("master", {"sslRequired": "NONE"})
        print("Successfully updated 'master' realm SSL settings.")
    except Exception as e:
        print(f"Warning: Could not update 'master' realm SSL settings: {e}")

    print(f"Updating '{KEYCLOAK_REALM}' realm sslRequired to 'NONE'...")
    try:
        keycloak_admin.update_realm(KEYCLOAK_REALM, {"sslRequired": "NONE"})
        print(f"Successfully updated '{KEYCLOAK_REALM}' realm SSL settings.")
    except Exception as e:
        print(f"Warning: Could not update '{KEYCLOAK_REALM}' realm SSL settings: {e}")

    # ----------------------------------------------------------------------
    # 2. Create the Realm (if not exists) - standard logic
    # ----------------------------------------------------------------------
    # ... (existing realm creation code) ...
    try:
        keycloak_admin.create_realm(payload={"realm": KEYCLOAK_REALM, "enabled": True})
        print(f"Realm '{KEYCLOAK_REALM}' created successfully.")
    except KeycloakError as e:
        if e.response_code == 409:
            print(f"Realm '{KEYCLOAK_REALM}' already exists.")
        else:
            print(f"Failed to create realm '{KEYCLOAK_REALM}': {e}")

    # ----------------------------------------------------------------------
    # 3. Create 'admin-siki' User with Full Privileges (Using Raw Requests)
    # ----------------------------------------------------------------------
    # We use raw requests here to ensure strict realm targeting, bypassing potential
    # KeycloakAdmin library context switching issues.

    print("Obtaining new admin token for user operations...")
    token = get_admin_token()

    cleanup_user_name = "admin-siki"

    # 3. Create in TARGET REALM
    print(f"Checking '{KEYCLOAK_REALM}' realm for user creation...")
    try:
        user_id = create_user(
            token, KEYCLOAK_REALM, cleanup_user_name, cleanup_user_name
        )  # pass used as username/pass

        # 4. Assign Roles
        if user_id:
            # Realm Role 'admin' - for general admin privileges within the realm
            assign_role(token, KEYCLOAK_REALM, user_id, "admin")
            # NOTE: Removed realm-management/realm-admin role - using groups for project permissions instead
    except Exception as e:
        print(f"Error creating/configuring user: {e}")

    # ----------------------------------------------------------------------
    # 5. Create 'timeIO-client' Client (standard logic)
    # ----------------------------------------------------------------------
    client_id = "timeIO-client"
    print(f"Checking/Creating client '{client_id}'...")

    # The original script's logic for direct access grants is still relevant
    # but might need to be adapted if the client creation is now handled by keycloak_admin.
    # For now, we'll keep the original direct access grants logic separate.
    token = get_admin_token()  # Re-authenticate with requests for the old functions

    client_uuid = get_client_id(token, CLIENT_ID_NAME)
    if client_uuid:
        print(f"Found client UUID: {client_uuid}")
        enable_direct_access_grants(token, client_uuid)
    else:
        print("Could not find client to update.")

    # ----------------------------------------------------------------------
    # 6. Seed Project Group Structure & Additional Users
    # ----------------------------------------------------------------------

    # Create Additional Users
    additional_users = ["SikiViewer", "SikiEditor", "Siki3"]
    user_ids_map = {}
    print("Creating additional users...")
    for u in additional_users:
        try:
            uid = create_user(token, KEYCLOAK_REALM, u, u)  # password = username
            if uid:
                user_ids_map[u] = uid
        except Exception as e:
            print(f"Error creating user {u}: {e}")

    # Define Projects and Assignments
    print(f"Seeding groups in '{KEYCLOAK_REALM}'...")

    projects_to_seed = ["UFZ-TSM:MyProject", "UFZ-TSM:MyProject2"]

    try:
        for project_group_name in projects_to_seed:
            # Create Main Group
            main_group_id = create_group(token, KEYCLOAK_REALM, project_group_name)

            if main_group_id:
                # Create Subgroups
                subgroups = ["Admin", "Viewer", "Editor"]
                subgroup_ids = {}
                for sub in subgroups:
                    sub_id = create_group(
                        token, KEYCLOAK_REALM, sub, parent_id=main_group_id
                    )
                    subgroup_ids[sub] = sub_id

                # Assign Users based on Project

                # 1. ALWAYS Assign 'admin-siki' to Main + Admin for EVERY project
                if user_id:  # admin-siki
                    if not user_id:  # re-fetch if lost context
                        user_id = find_user(token, KEYCLOAK_REALM, "admin-siki")["id"]
                    join_group(token, KEYCLOAK_REALM, user_id, main_group_id)
                    if subgroup_ids.get("Admin"):
                        join_group(
                            token, KEYCLOAK_REALM, user_id, subgroup_ids["Admin"]
                        )

                # 2. Specific User Assignments
                if project_group_name == "UFZ-TSM:MyProject":
                    # SikiViewer -> Main + Viewer
                    if "SikiViewer" in user_ids_map:
                        uid = user_ids_map["SikiViewer"]
                        join_group(token, KEYCLOAK_REALM, uid, main_group_id)
                        if subgroup_ids.get("Viewer"):
                            join_group(
                                token, KEYCLOAK_REALM, uid, subgroup_ids["Viewer"]
                            )

                    # SikiEditor -> Main + Editor
                    if "SikiEditor" in user_ids_map:
                        uid = user_ids_map["SikiEditor"]
                        join_group(token, KEYCLOAK_REALM, uid, main_group_id)
                        if subgroup_ids.get("Editor"):
                            join_group(
                                token, KEYCLOAK_REALM, uid, subgroup_ids["Editor"]
                            )

                elif project_group_name == "UFZ-TSM:MyProject2":
                    # Siki3 -> Main + Editor
                    if "Siki3" in user_ids_map:
                        uid = user_ids_map["Siki3"]
                        join_group(token, KEYCLOAK_REALM, uid, main_group_id)
                        if subgroup_ids.get("Editor"):
                            join_group(
                                token, KEYCLOAK_REALM, uid, subgroup_ids["Editor"]
                            )

    except Exception as e:
        print(f"Error seeding groups: {e}")
