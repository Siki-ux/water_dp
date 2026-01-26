from typing import Any, List

from fastapi import APIRouter, Body, Depends, HTTPException

from app.api.deps import get_current_user
from app.services.keycloak_service import KeycloakService
from app.services.project_service import ProjectService

router = APIRouter()


@router.get("/", response_model=List[Any])
def list_groups(user: dict = Depends(get_current_user)):
    """
    List groups.
    If user has 'admin' realm role, returns all groups.
    Otherwise, returns only groups the user is a member of.
    """
    user_id = user.get("sub")

    # Check for admin role
    is_admin = False
    realm_access = user.get("realm_access", {})
    if realm_access and "admin" in realm_access.get("roles", []):
        is_admin = True

    if is_admin:
        groups = KeycloakService.get_all_groups()
    else:
        groups = KeycloakService.get_user_groups(user_id)

    return groups


@router.get("/my-authorization-groups", response_model=List[Any])
def list_my_authorization_groups(user: dict = Depends(get_current_user)):
    """
    List groups where the user has specific authorization (e.g. is in /Editor or /Admin subgroup).
    Returns the distinct PARENT groups.
    """
    user_id = user.get("sub")
    user_groups = KeycloakService.get_user_groups(user_id)

    unique_parents = {}

    for group_item in user_groups:
        path = group_item.get("path", "")
        if not path:
            continue

        # Normalize path
        if path.startswith("/"):
            # Check for /Editor or /Admin suffix
            lower_path = path.lower()
            parent_path = None

            if lower_path.endswith("/editor"):
                parent_path = path[:-7]  # remove /editor
            elif lower_path.endswith("/admin"):
                parent_path = path[:-6]  # remove /admin

            if parent_path:
                parent_name = parent_path.split("/")[-1]

                # Deduplicate by parent path
                if parent_path not in unique_parents:
                    unique_parents[parent_path] = {
                        "name": parent_name,
                        "path": parent_path,
                        "id": None,  # ID is hard to get without extra query
                    }

    # Map path -> group for lookup
    path_map = {user_group.get("path"): user_group for user_group in user_groups}

    final_list = []
    for parent_path, parent_group_obj in unique_parents.items():
        if parent_path in path_map:
            # Great, user is direct member of parent appparently?
            # Or we just use the ID from there if it matches.
            final_list.append(
                {
                    "id": path_map[parent_path].get("id"),
                    "name": path_map[parent_path].get("name"),
                    "path": path_map[parent_path].get("path"),
                }
            )
        else:
            # User is in /Group/Editor but not /Group?
            # Optimization:
            found = KeycloakService.get_group_by_name(parent_group_obj["name"])
            if found:
                final_list.append(found)
            else:
                # Fallback
                final_list.append(parent_group_obj)

    return final_list


@router.post("/", status_code=201)
def create_group(
    name: str = Body(..., embed=True), user: dict = Depends(get_current_user)
):
    """
    Create a new Keycloak group.
    Prefixes the name with 'UFZ-TSM:' if not already present for Thing Management compatibility.
    Adds the creator to the group.
    """
    # Enforce UFZ-TSM prefix
    group_name = name
    if not group_name.startswith("UFZ-TSM:"):
        group_name = f"UFZ-TSM:{group_name}"

    # Check validity (Keycloak might reject duplicates)
    existing = KeycloakService.get_group_by_name(group_name)
    if existing:
        # If existing, user might just want to join? Or error?
        # For now, error if it exists.
        raise HTTPException(
            status_code=400, detail="Group with this name already exists"
        )

    group_id = KeycloakService.create_group(group_name)
    if not group_id:
        raise HTTPException(status_code=500, detail="Failed to create group")

    # Add creator to group
    user_id = user.get("sub")
    KeycloakService.add_user_to_group(user_id, group_id)

    # Assign 'user' role of 'timeIO-client' to the group
    try:
        timeio_client_uuid = KeycloakService.get_client_id("timeIO-client")
        if timeio_client_uuid:
            user_role = KeycloakService.get_client_role(timeio_client_uuid, "user")
            if user_role:
                KeycloakService.assign_group_client_roles(
                    group_id, timeio_client_uuid, [user_role]
                )
            else:
                # Log but don't fail the request significantly
                print("Warning: 'user' role not found for timeIO-client")
        else:
            print("Warning: timeIO-client not found")
    except Exception as error:
        # Just log error, don't break creation flow if possible
        print(f"Failed to assign client role: {error}")

    return {"id": group_id, "name": group_name, "status": "created"}


@router.get("/{group_id}", response_model=Any)
def get_group_details(group_id: str, user: dict = Depends(get_current_user)):
    """
    Get details of a specific group.
    """
    group = KeycloakService.get_group(group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return group


@router.get("/{group_id}/members", response_model=List[Any])
def get_group_members(
    group_id: str,
    exclude_admins: bool = True,
    user: dict = Depends(get_current_user),
):
    """
    Get members of a specific Keycloak group.

    - **exclude_admins**: If True (default), members of the 'Admin' subgroup will be excluded from the response.
    """
    members = KeycloakService.get_group_members(group_id)

    if exclude_admins:
        # Filter out members of 'Admin' subgroup
        try:
            admin_subgroup = KeycloakService.get_child_group(group_id, "Admin")
            if admin_subgroup:
                admin_members = KeycloakService.get_group_members(admin_subgroup["id"])
                admin_ids = {user_item["id"] for user_item in admin_members}

                # Filter members
                filtered_members = [member for member in members if member["id"] not in admin_ids]
                return filtered_members
        except Exception as error:
            print(f"Error filtering admin members: {error}")
            # Fallback to returning all members if filtering fails to avoid empty UI.
            pass

    return members


@router.post("/{group_id}/members", status_code=201)
def add_group_member(
    group_id: str,
    username: str = Body(..., embed=True),
    user: dict = Depends(get_current_user),
):
    """
    Add a user to a group by username.
    """
    if not ProjectService._is_admin(user):
        raise HTTPException(
            status_code=403, detail="Only Admins can manage group members"
        )

    target_user = KeycloakService.get_user_by_username(username)
    if not target_user:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")

    KeycloakService.add_user_to_group(user_id=target_user["id"], group_id=group_id)
    return {"status": "added", "user_id": target_user["id"]}


@router.delete("/{group_id}/members/{user_id}")
def remove_group_member(
    group_id: str, user_id: str, user: dict = Depends(get_current_user)
):
    """
    Remove a user from a group.
    """
    if not ProjectService._is_admin(user):
        raise HTTPException(
            status_code=403, detail="Only Admins can manage group members"
        )

    KeycloakService.remove_user_from_group(user_id=user_id, group_id=group_id)
    return {"status": "removed"}
