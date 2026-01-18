FROM registry.hzdr.de/ufz-tsm/thing-management/backend/thing-management-api:latest
# This image already has the necessary code for migrations (alembic)
# We just need to ensure the entrypoint or command is correct in compose
