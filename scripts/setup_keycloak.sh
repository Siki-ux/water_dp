#!/bin/bash
set -e

# Wait for Keycloak to be ready
echo "Waiting for Keycloak to start..."
sleep 20

# Authentication
echo "Authenticating with Keycloak..."
/opt/keycloak/bin/kcadm.sh config credentials --server http://keycloak:8080 --realm master --user ${KEYCLOAK_ADMIN} --password ${KEYCLOAK_ADMIN_PASSWORD}

# Disable SSL requirement
echo "Disabling SSL requirement..."
/opt/keycloak/bin/kcadm.sh update realms/master -s sslRequired=NONE
/opt/keycloak/bin/kcadm.sh update realms/timeio -s sslRequired=NONE

# Get Client ID
echo "Getting timeIO-client UUID..."
CLIENT_ID=$(/opt/keycloak/bin/kcadm.sh get clients -r timeio -q clientId=timeIO-client --fields id --format csv --noquotes)

if [ -z "$CLIENT_ID" ]; then
    echo "Error: timeIO-client not found!"
    exit 1
fi

echo "Found timeIO-client UUID: $CLIENT_ID"

# Create Protocol Mapper
# Helper function to delete mapper by name
delete_mapper_by_name() {
    local MAPPER_NAME=$1
    echo "Checking for mapper: $MAPPER_NAME"
    
    # Get all mappers with ID and Name in CSV
    # output format: id,name
    /opt/keycloak/bin/kcadm.sh get clients/$CLIENT_ID/protocol-mappers/models -r timeio --fields id,name --format csv --noquotes | while IFS=, read -r id name; do
        # Trim whitespace just in case
        id=$(echo "$id" | xargs)
        name=$(echo "$name" | xargs)
        
        if [ "$name" == "$MAPPER_NAME" ]; then
            echo "Found mapper '$name' with ID: $id. Deleting..."
            /opt/keycloak/bin/kcadm.sh delete clients/$CLIENT_ID/protocol-mappers/models/$id -r timeio
        fi
    done
}

# --------------------------------------------------------------------------------
# Create eduperson-entitlement-mapper
# --------------------------------------------------------------------------------
echo "Configuring eduperson-entitlement-mapper..."

# Delete existing (hyphenated)
delete_mapper_by_name "eduperson-entitlement-mapper"
# Delete conflicting (underscore)
delete_mapper_by_name "eduperson_entitlement"

echo "Creating new eduperson-entitlement-mapper..."
/opt/keycloak/bin/kcadm.sh create clients/$CLIENT_ID/protocol-mappers/models -r timeio -f - << EOF
{
  "name": "eduperson-entitlement-mapper",
  "protocol": "openid-connect",
  "protocolMapper": "oidc-hardcoded-claim-mapper",
  "consentRequired": false,
  "config": {
    "claim.name": "eduperson_entitlement",
    "claim.value": "[\"urn:geant:params:group:UFZ-TSM:MyProject\"]",
    "jsonType.label": "JSON",
    "id.token.claim": "true",
    "access.token.claim": "true",
    "userinfo.token.claim": "true"
  }
}
EOF

# --------------------------------------------------------------------------------
# Create eduperson_unique_id mapper (username)
# --------------------------------------------------------------------------------
echo "Configuring eduperson_unique_id mapper..."
delete_mapper_by_name "eduperson_unique_id"

/opt/keycloak/bin/kcadm.sh create clients/$CLIENT_ID/protocol-mappers/models -r timeio -f - << EOF
{
  "name": "eduperson_unique_id",
  "protocol": "openid-connect",
  "protocolMapper": "oidc-usermodel-property-mapper",
  "consentRequired": false,
  "config": {
    "user.attribute": "username",
    "id.token.claim": "true",
    "access.token.claim": "true",
    "claim.name": "eduperson_unique_id",
    "userinfo.token.claim": "true"
  }
}
EOF

# --------------------------------------------------------------------------------
# Create eduperson_principal_name mapper (email)
# --------------------------------------------------------------------------------
echo "Configuring eduperson_principal_name mapper..."
delete_mapper_by_name "eduperson_principal_name"

/opt/keycloak/bin/kcadm.sh create clients/$CLIENT_ID/protocol-mappers/models -r timeio -f - << EOF
{
  "name": "eduperson_principal_name",
  "protocol": "openid-connect",
  "protocolMapper": "oidc-usermodel-property-mapper",
  "consentRequired": false,
  "config": {
    "user.attribute": "email",
    "id.token.claim": "true",
    "access.token.claim": "true",
    "claim.name": "eduperson_principal_name",
    "userinfo.token.claim": "true"
  }
}
EOF

echo "Keycloak configuration completed successfully."
