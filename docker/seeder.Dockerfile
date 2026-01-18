FROM registry.hzdr.de/ufz-tsm/thing-management/backend/thing-management-api:latest

WORKDIR /app
COPY scripts/seed_thing_management.py /app/seed_thing_management.py

# Install any extra requirements if needed, or just use the base image env
# RUN pip install ...

CMD ["python", "/app/seed_thing_management.py"]
