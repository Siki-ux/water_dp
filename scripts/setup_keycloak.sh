#!/bin/bash
set -e

# Wait for Keycloak to be ready
echo "Waiting for Keycloak to start..."
sleep 20

# Authentication
echo "Authenticating with Keycloak..."
/opt/keycloak/bin/kcadm.sh config credentials --server http://keycloak:8080 --realm master --user admin --password admin

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
echo "Configuring eduperson-entitlement-mapper..."

# Check if mapper exists and delete it (hyphenated name)
MAPPER_ID=$(/opt/keycloak/bin/kcadm.sh get clients/$CLIENT_ID/protocol-mappers/models -r timeio -q name=eduperson-entitlement-mapper --fields id --format csv --noquotes)
if [ ! -z "$MAPPER_ID" ]; then
    echo "Mapper exists (ID: $MAPPER_ID). Deleting..."
    /opt/keycloak/bin/kcadm.sh delete clients/$CLIENT_ID/protocol-mappers/models/$MAPPER_ID -r timeio
fi

# Check for conflicting mapper (underscore name) and delete it
CONFLICT_MAPPER_ID=$(/opt/keycloak/bin/kcadm.sh get clients/$CLIENT_ID/protocol-mappers/models -r timeio -q name=eduperson_entitlement --fields id --format csv --noquotes)
if [ ! -z "$CONFLICT_MAPPER_ID" ]; then
    echo "Conflicting mapper exists (ID: $CONFLICT_MAPPER_ID). Deleting..."
    /opt/keycloak/bin/kcadm.sh delete clients/$CLIENT_ID/protocol-mappers/models/$CONFLICT_MAPPER_ID -r timeio
fi

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

echo "Keycloak configuration completed successfully."
