# Water Data Platform

A reliable Python backend for handling requests between databases, GeoServer, time data, and other services. Built with FastAPI, SQLAlchemy, and modern Python best practices.

## Features

- **Database Management**: PostgreSQL with PostGIS support for geospatial data.
- **GeoServer Integration**: Full integration with GeoServer for geospatial services (Layers & Features).
    - **Dynamic BBOX Filtering**: Efficiently load only visible map features.
    - **Single-Item Retrieval**: Fetch individual layers and features by ID.
    - **WMS/WFS Support**: Direct integration with standard OGC services.
- **Time Series Processing**: Powered by **TimeIO** (Frost Server + TimescaleDB).
    - **OGC SensorThings API**: Standardized data ingestion and management.
    - **High Performance**: TimescaleDB for efficient time-series storage and querying.
    - **Linkage**: Direct linking between Map Features (GeoServer) and Water Stations (`feature.properties.station_id`).
    - **Data API**: Retrieve, aggregate, and interpolate hydrological data.
- **RESTful API**: Comprehensive REST API with automatic documentation.
- **Data Models**: Strong typing with Pydantic V2 models (Schema) and SQLAlchemy ORM (DB).
- **Monitoring**: Built-in logging, metrics, and health checks.
- **Docker Support**: Complete containerization with Docker Compose.

## Architecture

The platform follows a modern microservices-inspired architecture:

### 1. Data Layer
- **TimescaleDB**: The central database. Extends PostgreSQL to handle both:
    - **Relational Data**: Geospatial features, application state.
    - **Time Series Data**: High-volume sensor readings (Observations).
- **MinIO**: S3-compatible object storage (component of TimeIO stack).

### 2. TimeIO Stack (Sensor Data)
- **Frost Server**: Implements the OGC SensorThings API standards.
    - **Role**: Ingestion point for all sensor data.
    - **Flow**: `Sensors -> Frost API (HTTP/MQTT) -> TimescaleDB`.
- **Thing Management**: UI and API for managing Things, Sensors, and Datastreams.
- **Keycloak**: IAM for securing TimeIO services.
- **MQTT Broker**: Real-time message bus for sensor data ingestion.

### 3. Application Layer
- **FastAPI Backend (Water DP)**:
    - **Role**: Main application logic, serving the frontend and orchestrating data.
    - **Integration**: Queries TimescaleDB directly for analytics (anomaly detection) and lists metadata from the database.
- **GeoServer**:
    - **Role**: Serves geospatial maps (WMS/WFS).
    - **Integration**: Connects to PostGIS tables in TimescaleDB to serve vector layers (e.g., rivers, regions).

### 4. Visualization
- **Grafana**:
    - **Role**: Interactive dashboards for time-series data.
    - **Integration**: Connects directly to TimescaleDB to visualize seeded `OBSERVATIONS`.

## TimeIO Integration

### Why TimeIO?
The project integrates the [TimeIO](https://helmholtz.software/software/timeio) stack to provide a robust, standardized, and scalable solution for handling sensor data.
1.  **Standardization**: Uses the **OGC SensorThings API** (via Frost Server) for data ingestion. This ensures interoperability and a clear schema for Things, Sensors, and Observations.
2.  **Scalability**: Leverages **TimescaleDB** (PostgreSQL extension) for efficient storage and querying of high-frequency time-series data.
3.  **Separation of Concerns**: Decouples data ingestion (IoT devices -> Frost) from application logic (FastAPI -> DB), allowing independent scaling.

### Data Flow
1.  **Ingestion**: 
    - IoT devices or scripts (e.g., `seed_timeio.py`) send data to the **Frost Server** via HTTP or MQTT.
    - Frost validates the data against the OGC model.
2.  **Persistence**:
    - Frost persists the data into **TimescaleDB** (`OBSERVATIONS` table).
    - It handles efficient partitioning and indexing automatically.
    - **MinIO** is used for object storage if needed (e.g., large metadata).
3.  **Consumption**:
    - **FastAPI**: Queries the `OBSERVATIONS` table directly (using raw SQL for performance) to provide analytics endpoints (e.g., `/statistics`, `/anomalies`).
    - **Grafana**: Connects to the same DB to visualize the raw data.
    - **GeoServer**: Joins spatial features with sensor metadata (using `station_id`) to display real-time status on maps.

### Setup Details
The entire stack is containerized in `docker-compose.yml`. Key configuration files:
- `.env`: General application secrets.
- `timeio.env`: Specific configuration for the TimeIO microservices (Auth, DB, MQTT).
    - *Tip*: Use `timeio.env.example` as a template.
- `init.sql`: Ensures `timescaledb` and `postgis` extensions are enabled on startup.

```mermaid
graph TD
    User[User/Client] --> API[FastAPI Backend]
    User --> GeoServer
    User --> Grafana
    User --> ThingMgmt[Thing Management UI]

    API -->|Read/Write| DB[(TimescaleDB)]
    GeoServer -->|Read Geo Data| DB
    Grafana -->|Read Time Series| DB

    Sensor[Sensors/Seed Script] -->|HTTP/MQTT| Frost[Frost Server]
    Frost -->|Persist| DB
    ThingMgmt --> Frost
    ThingMgmt --> Frost
```

## Security Considerations

> [!WARNING]
> This repository contains default configurations intended for **local development only**.

### 1. Default Credentials
The `timeio.env.example` and `timeio-realm.json` files contain default passwords (e.g., `admin/admin`). **These must be changed before deploying to any production environment.**

- **Root Credentials**: `POSTGRES_PASSWORD`, `MINIO_ROOT_PASSWORD`, and `GEOSERVER_ADMIN_PASSWORD` should be randomized.
- **Seeded Users**: The `frontendbus` and `siki` users seeded by `scripts/configure_keycloak_realm.py` are set with **temporary passwords** by default. Any login with these credentials will force an immediate password change. In production, consider removing these seeded users entirely or defining them with unique, secure secrets via environment variables.
### 2. CORS and Redirect URIs
- **CORS**: `CORS_ORIGINS` defaults to `*` to facilitate local development. In production, set this to the specific domain(s) of your frontend application in `.env`.
- **CORS**: The default configuration (specifically `FROST_HTTP_CORS_ALLOWED_ORIGINS` in `docker-compose.yml`) allows `*` (all origins) or `localhost` by default. Update this for production.
- **Keycloak**: The default `timeio-realm.json` sets `webOrigins` to `+`, allowing any origin matching redirect URIs. Overwrite this in production with specific allowed origins (e.g. `https://your-domain.com`) to prevent CORS attacks.
- **Keycloak Redirects**: The default Keycloak configuration allows redirects to localhost ports (8000, 8080, 8082, 3000). Ensure `scripts/configure_keycloak_realm.py` or your realm configuration is updated to reflect your actual production domains and ports.

## Quick Start

### Prerequisites

- Docker & Docker Compose
- (Optional) Python 3.11+ for local script execution

### Installation & Run

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd water_dp
   ```
   
2. **Set up environment**
   ```bash
   # Copy environment templates
   cp env.example .env
   cp timeio.env.example timeio.env
   
   # Edit .env and timeio.env if needed.
   # NOTE: timeio.env configures the entire TimeIO stack (DB, Auth, MQTT).

   > [!IMPORTANT]
   > The `timeio.env.example` file contains default credentials (e.g., `admin/admin`) for development convenience.
   > **YOU MUST CHANGE THESE CREDENTIALS IN `timeio.env` BEFORE DEPLOYING TO A PRODUCTION ENVIRONMENT.**
   > Failure to do so will create significant security vulnerabilities.
   ```

3. **Start all services with Docker**
   ```bash
   docker-compose up -d --build
   ```
   This will start a comprehensive stack:
   - **Infrastructure**: TimescaleDB (5432), MinIO, MQTT Broker (1883), Keycloak (8081).
   - **TimeIO**: Frost API (8083), Thing Management (8082).
   - **Application**: GeoServer (8080), FastAPI (8000), Grafana (3000).

4. **Seed Data (Important)**
   To populate the system with initial data (Things, Sensors, Observations, GeoServer Layers), run the reset script:
   ```bash
   # Make sure services are up first
   docker-compose ps
   
   # Run the seeder (requires poetry)
   poetry install
   poetry run python -m app.reset_and_seed
   ```
   *This script drops existing tables, re-initializes schema, and seeds standard stations and time-series data.*

5. **Access the application**
   - **API Docs**: [http://localhost:8000/api/v1/docs](http://localhost:8000/api/v1/docs)
   - **GeoServer Admin**: [http://localhost:8080/geoserver](http://localhost:8080/geoserver) (Default: `admin` / `geoserver`)
   - **Thing Management**: [http://localhost:8082/things](http://localhost:8082/things) (Default: `admin` / `admin` via Keycloak)
   - **Frost API**: [http://localhost:8083/FROST-Server](http://localhost:8083/FROST-Server)
   - **Grafana**: [http://localhost:3000/login](http://localhost:3000/login) (Default: `admin` / `admin`)

### Verification
To verify that the entire stack is working correctly (Layers, Features, Data Linkage, and Time Series content), run the included verification script:

```bash
# Using poetry (recommended)
poetry run python scripts/verify_api.py
```

This script will check:
1.  **Layer Listing**: Confirms `czech_regions` and `czech_republic` layers exist.
2.  **Feature Retrieval**: Fetches features and checks for `station_id` property.
3.  **Data Linkage**: Uses the linked `station_id` to fetch time series metadata and data points.

## Development & Testing

This project uses **Poetry** for dependency management.

### Setup
```bash
# Install dependencies
poetry install

# Activate shell
poetry shell
```

### Running Tests
The project includes a comprehensive test suite using `pytest`.

```bash
# Run Unit Tests (Fast, Mocked) - Default
poetry run pytest

# Run Integration Tests (Requires running stack)
# Ensure docker-compose is up
poetry run pytest tests/integration
```

### Code Structure
- **Services**: Business logic is encapsulated in `app/services/`.
- **Schemas**: Pydantic V2 models (`ConfigDict`, `field_validator`) in `app/schemas/`.
- **Models**: SQLAlchemy models in `app/models/`.

## API Usage Guide

### 1. Water Data (Stations)
- **List Stations**: `GET /api/v1/water-data/stations` (Returns combined Geo + FROST data)

### 2. Geospatial Data
- **List Layers**: `GET /api/v1/geospatial/layers`
- **Get Features (with BBOX)**: `GET /api/v1/geospatial/features?layer_name=czech_regions&bbox=12.0,48.5,18.9,51.1`
- **Get Single Feature**: `GET /api/v1/geospatial/features/{feature_id}?layer_name=czech_regions`

### 3. Time Series Data
- **List Series**: `GET /api/v1/time-series/metadata`
- **Get Data**: `GET /api/v1/time-series/data?series_id={from_metadata_or_feature}`

## Database Migrations

This project uses **Alembic** for managing database schema changes.

### Running Migrations
To apply existing migrations (e.g., when pulling new code):
```bash
poetry run alembic upgrade head
```

### Creating Migrations
When you modify models in `app/models/`, create a new migration script:
```bash
# 1. Generate revision
poetry run alembic revision --autogenerate -m "Description of changes"

# 2. Review the generated file in alembic/versions/

# 3. Apply the migration
poetry run alembic upgrade head
```

## TODO:

### TimeIO
- [x] Use TimeIO to replace the custom time-series storage engine
- [ ] Use Keycloak for authentication properly
### Security
- [ ] Fix security problems with project

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
