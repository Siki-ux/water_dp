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
- **Smart Data Management**: 
    - **Smart ID Resolution**: Access sensors via internal UUIDs or original string IDs (`STATION_123`).
    - **Auto-Provisioning**: Automatically creates Locations and Features of Interest during data import.
    - **Bulk Operations**: Efficiently import large historical datasets and delete recursively.

## Architecture

The platform follows a modern microservices-inspired architecture:

### Project Structure
```
water_dp/
├── app/
│   ├── api/                 # API endpoints (Geospatial, Time Series, Water Data)
│   │   └── v1/
│   │       └── endpoints/
│   │           ├── bulk.py          # Bulk import endpoints
│   │           └── computations.py   # Computation execution endpoints
│   ├── core/               # Core functionality (Config, DB, Logging, Seeding, Security)
│   │   └── celery_app.py   # Celery application configuration
│   ├── models/             # SQLAlchemy Database models
│   │   └── computations.py # ComputationScript & ComputationJob models
│   ├── schemas/            # Pydantic validation schemas
│   ├── services/           # Business logic
│   │   ├── database_service.py    # CRUD for all entities
│   │   ├── geoserver_service.py   # GeoServer interaction
│   │   └── time_series_service.py # TimeIO integration
│   ├── tasks/              # Celery background tasks
│   │   ├── import_tasks.py        # Bulk import tasks
│   │   └── computation_tasks.py   # Computation execution tasks
│   ├── computations/       # Python scripts for heavy computation
│   └── main.py            # FastAPI application entry point
├── tests/                 # Unit and integration tests
│   ├── test_services/     # Service layer tests
│   ├── integration/       # Integration tests
│   ├── test_api_bulk.py # API tests for bulk import
│   ├── test_api_computations.py # API tests for computations
│   └── conftest.py        # Test configuration
├── scripts/               # Utility scripts (Verification, Seeding)
├── keycloak/             # Keycloak realm configuration
├── grafana/              # Grafana provisioning
├── alembic/              # Database migrations
├── pyproject.toml        # Poetry dependencies and config
├── docker-compose.yml    # Docker services orchestration
├── Dockerfile           # Application container definition
└── README.md            # This file
```

### 1. Data Layer
- **PostgreSQL with Extensions**: The central database:
    - **PostGIS**: Geospatial data (rivers, regions, boundaries)
    - **TimescaleDB**: Time-series data (high-volume sensor readings)
- **MinIO**: S3-compatible object storage (component of TimeIO stack)
- **Redis**: Caching and message broker

### 2. TimeIO Stack (Sensor Data)
- **Frost Server**: Implements the OGC SensorThings API standards
    - **Role**: Ingestion point for all sensor data
    - **Flow**: `Sensors -> Frost API (HTTP/MQTT) -> TimescaleDB`
- **Thing Management**: UI and API for managing Things, Sensors, and Datastreams
- **Keycloak**: IAM for securing TimeIO services
- **MQTT Broker**: Real-time message bus for sensor data ingestion

### 3. Application Layer
- **FastAPI Backend (Water DP)**:
    - **Role**: Main application logic, serving the frontend and orchestrating data.
    - **Security**: Protected by **Keycloak** (JWT Bearer Token authentication).
    - **Integration**: Queries TimescaleDB directly for analytics (anomaly detection) and lists metadata from the database.
- **GeoServer**:
    - **Role**: Serves geospatial maps (WMS/WFS)
    - **Integration**: Connects to PostGIS tables in PostgreSQL to serve vector layers (e.g., rivers, regions)

### 4. Visualization
- **Grafana**:
    - **Role**: Interactive dashboards for time-series data
    - **Integration**: Connects directly to TimescaleDB to visualize seeded `OBSERVATIONS`

### 5. Database Schema

The database is shared but logically segmented into three distinct domains. The usage of this hybrid schema allows us to separate **Application Logic** from **Data Ingestion** and **High-Volume Storage**.

#### 1. Water DP (User Context & GIS)
These tables manage the modern application state, user projects, and geospatial layers.
- **projects**: The central user workspace. Linked to Keycloak users via `owner_id`.
- **project_sensors**: A lightweight link table connecting a **Project** (UUID) to a **TimeIO Sensor** (String ID).
- **geo_layers / geo_features**: PostGIS-enabled tables for storing map configurations and geometries (polygons, points).
- **computation_scripts / computation_jobs**: Store user-uploaded Python scripts and track the status of asynchronous Celery tasks.

#### 2. Thing Management (Ingestion Config)
These legacy tables are used by the **Thing Management API** to configure how data enters the system.
- **project_tbl** (Legacy): The "backend" view of a project. Used to group database credentials.
- **mqtt / database**: Stores credentials for the ingestion services (MQTT Broker, Timescale Writer).

#### 3. TimeIO / OGC (Sensor Data)
These tables implement the **OGC SensorThings API** standard and are managed by the **Frost Server**.
- **THINGS**: Represents the physical or virtual station.
- **DATASTREAMS**: A stream of data (e.g., "Water Level") associated with a Thing.
- **OBSERVATIONS**: The actual time-series data points (Timestamp + Value), stored in **TimescaleDB** hypertables for performance.

```mermaid
erDiagram
    %% --- Domain: Water DP (Application) ---
    PROJECTS {
        uuid id PK "New Project ID"
        string name
        string owner_id "Keycloak User ID"
    }
    
    DASHBOARDS {
        uuid id PK
        uuid project_id FK
        json layout_config
    }
    
    COMPUTATION_SCRIPTS {
        uuid id PK
        uuid project_id FK
        string name
        string filename
    }

    COMPUTATION_JOBS {
        string id PK "Celery Task ID"
        uuid script_id FK
        string status
    }
    
    PROJECT_SENSORS {
        uuid project_id PK, FK
        string sensor_id PK "TimeIO String ID"
    }

    GEO_LAYERS ||--o{ GEO_FEATURES : contains

    %% --- Domain: Thing Management (Ingestion) ---
    PROJECT_TBL {
        int id PK "Legacy ID"
        uuid uuid "Sync ID"
        string name
    }
    
    MQTT_CONFIG {
        int id PK
        int project_id FK
        string topic
        string user
    }

    %% --- Domain: TimeIO / OGC (Data) ---
    THINGS {
        bigint id PK "Internal ID"
        string name "Unique Name"
        json properties "Contains station_id"
    }
    
    DATASTREAMS {
        bigint id PK
        bigint thing_id FK
    }
    
    OBSERVATIONS {
        bigint id PK
        bigint datastream_id FK
        timestamp phenomenonTime
        double result
    }

    %% --- Relationships ---
    
    %% App User Context
    PROJECTS ||--o{ DASHBOARDS : contains
    PROJECTS ||--o{ PROJECT_SENSORS : links_to
    
    %% Logical Link: App -> TimeIO
    PROJECT_SENSORS }|..|| THINGS : "Refers to (by Name/Prop)"

    %% Ingestion Config
    PROJECT_TBL ||--o{ MQTT_CONFIG : owns

    %% OGC Hierarchy
    THINGS ||--o{ DATASTREAMS : has
    DATASTREAMS ||--o{ OBSERVATIONS : contains
```

### System Architecture Diagram

```mermaid
graph TB
    subgraph "Client Layer"
        User[User/Client]
        Frontend[Frontend App]
    end
    
    subgraph "Application Layer"
        API[FastAPI Backend<br/>Water DP]
        Worker[Celery Worker<br/>Async Tasks]
        GeoServer[GeoServer<br/>WMS/WFS]
        Grafana[Grafana<br/>Dashboards]
        ThingMgmt[Thing Management<br/>UI]
    end
    
    subgraph "TimeIO Stack"
        Frost[Frost Server<br/>OGC SensorThings API]
        MQTT[MQTT Broker<br/>Mosquitto]
        Keycloak[Keycloak<br/>IAM]
    end
    
    subgraph "Data Layer"
        DB[(PostgreSQL<br/>+ PostGIS<br/>+ TimescaleDB)]
        Redis[(Redis<br/>Cache & Queue)]
        MinIO[(MinIO<br/>Object Storage)]
    end
    
    subgraph "Data Sources"
        Sensors[IoT Sensors<br/>Water Stations]
    end
    
    %% User connections
    User --> API
    User --> GeoServer
    User --> Grafana
    User --> ThingMgmt
    Frontend --> API
    Frontend --> GeoServer
    
    %% Application layer connections
    API -->|Read/Write| DB
    API -->|Cache/Queue| Redis
    Worker -->|Consume Tasks| Redis
    Worker -->|Read/Write| DB
    Worker -->|Run Scripts| Computations[Computation Scripts]
    GeoServer -->|Read Geo Data<br/>PostGIS| DB
    Grafana -->|Read Time Series<br/>TimescaleDB| DB
    ThingMgmt -->|Manage Things| Frost
    
    %% TimeIO connections
    Frost -->|Persist| DB
    Frost -->|Metadata| MinIO
    Frost -.->|Authenticate| Keycloak
    ThingMgmt -.->|Authenticate| Keycloak
    
    %% Sensor data flow
    Sensors -->|HTTP/MQTT| MQTT
    MQTT --> Frost
    Sensors -->|HTTP| Frost
```

## TimeIO & Frontend Architecture Strategy
*(Added Jan 2026)*

To accelerate development and leverage existing robust tools, we have adopted a **"Microservice UI Composition"** strategy for Device Management.

### Decision
Instead of rebuilding complex device management interfaces (Parser config, Access Policies, Metadata editing) in the `hydro_portal`, we **reuse** the existing [TimeIO Thing Management](https://helmholtz.software/software/timeio) UI.

### Integration Workflow
1.  **Device Provisioning**: Performed in **TimeIO TM** (`http://localhost:8082`).
    *   Admins create Things, Datastreams, and Parsers here.
2.  **Visualization & Projects**: Performed in **Hydro Portal** (`http://localhost:3000`).
    *   Users **"Link"** existing TimeIO Things to their Projects instead of creating them from scratch.
    *   Hydro Portal focuses purely on Dashboards, Maps, and Analytics.
3.  **Single Sign-On (SSO)**:
    *   Both applications share the same **Keycloak** Identity Provider.
    *   Users seamlessly switch between "Dashboards" (Hydro Portal) and "Device Settings" (TimeIO TM) without re-authenticating.

### Implementation Details
*   **Frontend**: `hydro_portal` removes "Create Sensor" forms in favor of a "Link Sensor" modal (listing things from `GET /things`).
*   **Navigation**: "Manage Devices" links in Hydro Portal redirect to TimeIO TM.
*   **Keycloak Client**: The `timeIO-client` is configured to allow redirects to both `http://localhost:3000` and `http://localhost:8082`.


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

## Security Considerations

> [!WARNING]
> This repository contains default configurations intended for **local development only**.

### 1. Default Credentials
The `timeio.env.example` and `timeio-realm.json` files contain default passwords (e.g., `admin/admin`). **These must be changed before deploying to any production environment.**

### 2. CORS and Redirect URIs
- **CORS**: `CORS_ORIGINS` defaults to `*` to facilitate local development. In production, set this to the specific domain(s) of your frontend application in `.env`.
- **Keycloak Redirects**: The default Keycloak configuration allows redirects to localhost ports (8000, 8080, 8082, 3000). Ensure `scripts/configure_keycloak_realm.py` or your realm configuration is updated to reflect your actual production domains and ports.

### 3. Hardcoded Credentials & Roles
The system is seeded with default users and roles (e.g., in `scripts/configure_keycloak_realm.py`) for ease of development. 

> [!CRITICAL]
> **Production Safety**: You **MUST** modify these hardcoded users, passwords, and role assignments before deploying to any production environment. The default `admin-siki` and `frontendbus` users provide extensive access that would be dangerous if left unchanged.

### 4. Sensor Discovery & Sharing (Trusted Gateway Pattern)
The platform is designed as a **Trusted Research/Internal Workspace**, which influences the sensor security model:

*   **Open Discovery**: Any authenticated user can "discover" and list *all* available sensors within the underlying TimeIO infrastructure. This is by design, treating sensors as shared public infrastructure (e.g., widely used River Gauges).
*   **Permissive Linking**: There is currently no strict "Sensor Ownership" enforcement at the Project level. Any user can link any available sensor to their project to view its data.
*   **Project Privacy**: While sensors are public, **Projects are Private**. User A cannot view User B’s Project dashboard, analysis, or computed data.
*   **Production Note**: If multi-tenant isolation is required (where User A cannot even *see* User B's sensors), the `link_sensor` API would need to enforce ownership checks against the TimeIO Management API.

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

   **Thing Management Seeding:**
   The `timeio_tm_seeder` service automatically runs `scripts/seed_thing_management.py` on startup. This script:
   - Patches the Thing Management database to ensure compatibility with the API.
   - Syncs sensors from FROST Server (e.g., `Auto-Simulated Sensor`) into Thing Management.
   - Links "MyProject" and creates default parsers.
   - Enables simulation for imported sensors.

5. **Access the application**
   - **API Docs**: [http://localhost:8000/api/v1/docs](http://localhost:8000/api/v1/docs)
   - **GeoServer Admin**: [http://localhost:8080/geoserver](http://localhost:8080/geoserver) (Default: `admin` / `geoserver`)
   - **Thing Management**: [http://localhost:8082/things](http://localhost:8082/things) (Default: `admin` / `admin` via Keycloak)
   - **Frost API**: [http://localhost:8083/FROST-Server](http://localhost:8083/FROST-Server)
   - **Grafana**: [http://localhost:3000/login](http://localhost:3000/login) (Default: `admin` / `admin`)
   - **Keycloak Console**: [http://localhost:8081/admin](http://localhost:8081/admin) (Default: `admin` / `admin`)
   
   **Default Application Users (Development Only):**
   - **Admin User**: `admin-siki` / `admin-password` (Full API access)
   - **Frontend User**: `frontendbus` / `frontend-password` (Limited access)

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

> [!NOTE]
> Database seeding is automatically disabled during unit tests (`conftest.py` fixture) for speed.

```bash
# Run Unit Tests (Fast, Mocked) - Default (excludes integration)
poetry run pytest

# Run Integration Tests (Requires running stack)
poetry run pytest -m integration

# Run Specific Test Categories (Markers)
poetry run pytest -m api
poetry run pytest -m services
poetry run pytest -m core

# Run Coverage Tests
poetry run pytest tests/test_services/test_time_series_service_coverage.py
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

### 4. Bulk Import
- **Import GeoJSON**: `POST /api/v1/bulk/import/geojson` (Upload file)
- **Import Time Series**: `POST /api/v1/bulk/import/timeseries` (Upload JSON/CSV)
- **Check Task Status**: `GET /api/v1/bulk/tasks/{task_id}`

### 5. Computations
- **Upload Script**: `POST /api/v1/computations/upload` (Requires Editor Role)
- **Run Script**: `POST /api/v1/computations/run/{script_id}`
- **Get Job Status**: `GET /api/v1/computations/tasks/{task_id}`

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

## Roadmap: Dynamic Frontend Support
To support a highly customizable frontend (dashboards, maps, sub-portals) with predictive capabilities, the current backend requires the following architectural additions:

### Gap Analysis


1.  **Computation & Prediction Engine (The "Complex Computation" Part)**
    - **Missing**: Current analytics are limited to fast SQL aggregations (min/max/avg). There is no infrastructure for running heavy prediction models (AI/ML) or long-running simulations.
    - **Why**: "Predictions on how water will behave" requires complex mathematics that cannot run inside a standard HTTP request.
    - **Need**:
        - **Job Queue** (e.g., Celery/Redis) for asynchronous processing.
        - **Scheduler** for running regular predictions (e.g., "every night at 00:00").
        - **Prediction Service** to interface with ML libraries (scikit-learn, etc.).

2.  **Bulk Data Import (The "Efficient Loading" Part)**
    - **Missing**: No API endpoints or utilities for bulk importing large datasets.
    - **Why**: Initial setup, data migration, or historical data loading requires efficiently inserting thousands/millions of records.
    - **Need**:
        - **Bulk GeoJSON Import**: Load large geographic datasets into PostGIS/GeoServer.
        - **Bulk Time-Series Import**: Efficiently insert millions of sensor readings into TimescaleDB.
        - **CSV/Parquet Support**: Common data formats for hydrology data.
        - **Background Processing**: Use job queue for large imports to avoid timeout.

3.### Gap Analysis for Hydro_portal Frontend (Updated)
    - **Spatial Analysis**: The `spatial_query` endpoint exists but requires further implementation for complex polygon intersections.
    - **Project & Dashboard Management**:
        - **Status**: Backend supports Project-based sharing. Frontend now includes Data Management tabs (Sensors vs Datasets).
    - **User Registration**: Currently handled via Keycloak Admin Console. A public registration endpoint is planned.
    - **Fixed Items**:
        - **Bulk Data Import**: Now fully implemented for both GeoJSON and Time-Series CSVs.
        - **Sensor Status**: Logic to determine status added to API.
        - **Static Data Support**: Added "Dataset" type for virtual/imported data sources.

### TODO
- [x] **User Config Store**: Design DB schema and API for `UserDashboards` and `WidgetConfigs` (JSONB).
- [x] **Computation Infrastructure**: Set up a Worker Queue (Celery) and Redis for background tasks.
- [x] **Prediction Engine**: Implemented `ComputationScript` engine to run Python scripts (e.g., simulations/predictions) on Worker nodes.
- [x] **Logical Grouping**: Add `Project` / `Group` tables to organize Resources (Layers, Sensors) into "Apps".
- [x] **Security**: Implement FastAPI Middleware to validate Keycloak Tokens (Validation) and enforce scopes/roles.
- [x] **Bulk Import**: Create endpoints and utilities for bulk data import (CSV, GeoJSON, Parquet) with background job processing.

### Implementation Plan
To achieve the above goals, we will implement the following:

#### 1. Architecture: The "Worker" Service (Implemented)
We decoupled heavy computations from the main API.
-   **Added container**: `worker` in `docker-compose.yml`.
-   **Technology**: [Celery](https://docs.celeryq.dev/) (Distributed Task Queue).
-   **Broker**: [Redis](https://redis.io/) (Already present in stack).
-   **Workflow**:
    1.  User requests a "Flood Prediction" via API -> `POST /api/v1/jobs/predict`.
    2.  API pushes a task to Redis Queue.
    3.  Worker picks up the task, fetches data from TimeIO, runs the model, and writes results back to TimeIO.



#### 2. Technology Stack Enhancements
-   **Job Queue**: `celery` + `redis`
-   **Machine Learning**: `scikit-learn` or `prophet` (inside the `worker` container).
-   **JSON Storage**: SQLAlchemy `JSONB` type (for flexible dashboard layouts).

#### 3. Bulk Data Import Tools (Implemented)
We created efficient tools for loading large datasets without blocking the API.
-   **API Endpoints**:
    - `POST /api/v1/bulk/import/geojson` - Upload GeoJSON files for PostGIS/GeoServer
    - `POST /api/v1/bulk/import/timeseries` - Upload CSV with sensor data to TimescaleDB
    - `POST /api/v1/computations/run/{script}` - Trigger background scripts.
-   **Technologies**:
    - `pandas` (already present) for CSV/Parquet parsing
    - `geopandas` for GeoJSON processing and validation
    - Celery for background processing (large files)
-   **Workflow**:
    1. Upload file via API (file validation and size check)
    2. Push job to Celery queue (returns job ID immediately)
    3. Worker processes file in background
    4. Client polls `/api/v1/bulk/tasks/{job_id}` for progress/completion

### Fixes
- [x] Use TimeIO to replace the custom time-series storage engine
- [x] Use Keycloak for authentication properly
- [x] Fix security problems with project

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
