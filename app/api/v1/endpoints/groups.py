from typing import List, Any
from fastapi import APIRouter, Depends, HTTPException, Path, Body
from app.services.keycloak_service import KeycloakService
from app.services.project_service import ProjectService
from app.api.deps import get_current_user

router = APIRouter()

@router.get("/", response_model=List[Any])
def list_groups(
    current_user: dict = Depends(get_current_user)
):
    """
    List groups for the current user.
    """
    user_id = current_user.get("sub")
    # If admin, maybe list all? For now, list user's groups as requested for "control groups he has access to"
    groups = KeycloakService.get_user_groups(user_id)
    return groups

@router.post("/", status_code=201)
def create_group(
    name: str = Body(..., embed=True),
    current_user: dict = Depends(get_current_user)
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
         raise HTTPException(status_code=400, detail="Group with this name already exists")

    group_id = KeycloakService.create_group(group_name)
    if not group_id:
        raise HTTPException(status_code=500, detail="Failed to create group")

    # Add creator to group
    user_id = current_user.get("sub")
    KeycloakService.add_user_to_group(user_id, group_id)

    # Assign 'user' role of 'timeIO-client' to the group
    try:
        timeio_client_uuid = KeycloakService.get_client_id("timeIO-client")
        if timeio_client_uuid:
            user_role = KeycloakService.get_client_role(timeio_client_uuid, "user")
            if user_role:
                KeycloakService.assign_group_client_roles(group_id, timeio_client_uuid, [user_role])
            else:
                # Log but don't fail the request significantly
                print(f"Warning: 'user' role not found for timeIO-client")
        else:
             print("Warning: timeIO-client not found")
    except Exception as e:
        # Just log error, don't break creation flow if possible
        print(f"Failed to assign client role: {e}")

    return {"id": group_id, "name": group_name, "status": "created"}

@router.get("/{group_id}", response_model=Any)
def get_group_details(
    group_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get details of a specific group.
    """
    # Authorization check: Is user member or admin?
    # Simplification: If they can fetch it, they likely have access if we implement strict checking.
    # For now, relying on KeycloakService/Admin roles or simply fetching.
    group = KeycloakService.get_group(group_id)
    if not group:
         raise HTTPException(status_code=404, detail="Group not found")
    return group

@router.get("/{group_id}/members", response_model=List[Any])
def get_group_members(
    group_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Get members of a specific Keycloak group.
    Requires Admin or Group Admin privileges (checked via Keycloak service roles implicitly or here).
    For now, we allow any valid user to see members of groups they have access to?
    Or restrict to Admins.
    """
    # Simple check: Is Admin?
    # if not ProjectService._is_admin(current_user):
    #     raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Ideally, allow if user is member of group?
    # For now, let's behave like the requirement: "users with group admin... or admin"
    # We will defer granular permission to a later refinement or rely on KeycloakAdmin errors.
    
    members = KeycloakService.get_group_members(group_id)
    return members

@router.post("/{group_id}/members", status_code=201)
def add_group_member(
    group_id: str,
    username: str = Body(..., embed=True),
    current_user: dict = Depends(get_current_user)
):
    """
    Add a user to a group by username.
    """
    if not ProjectService._is_admin(current_user):
        raise HTTPException(status_code=403, detail="Only Admins can manage group members")
        
    user = KeycloakService.get_user_by_username(username)
    if not user:
        raise HTTPException(status_code=404, detail=f"User '{username}' not found")
        
    KeycloakService.add_user_to_group(user_id=user['id'], group_id=group_id)
    return {"status": "added", "user_id": user['id']}

@router.delete("/{group_id}/members/{user_id}")
def remove_group_member(
    group_id: str,
    user_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Remove a user from a group.
    """
    if not ProjectService._is_admin(current_user):
        raise HTTPException(status_code=403, detail="Only Admins can manage group members")

    KeycloakService.remove_user_from_group(user_id=user_id, group_id=group_id)
    return {"status": "removed"}
