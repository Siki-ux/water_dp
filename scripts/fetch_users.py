import os

import requests


def get_users():
    token_url = "http://keycloak:8080/realms/master/protocol/openid-connect/token"
    # Admin CLI credentials
    data = {
        "client_id": "admin-cli",
        "username": os.getenv("KEYCLOAK_ADMIN_USERNAME", "admin"),
        "password": os.getenv("KEYCLOAK_ADMIN_PASSWORD", "admin"),
        "grant_type": "password",
    }

    try:
        r = requests.post(token_url, data=data)
        r.raise_for_status()
        token = r.json()["access_token"]

        users_url = "http://keycloak:8080/admin/realms/timeio/users"
        headers = {"Authorization": f"Bearer {token}"}

        r_users = requests.get(users_url, headers=headers)
        r_users.raise_for_status()

        users = r_users.json()
        print(f"Found {len(users)} users:")
        for u in users:
            print(f"User: {u.get('username')} | ID: {u.get('id')}")

    except Exception as e:
        print(f"Error: {e}")
        # Only print response text if requests failed
        pass


if __name__ == "__main__":
    get_users()
