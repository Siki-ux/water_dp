# Water Data Platform

A reliable Python backend for handling requests between databases, GeoServer, time data, and other services. Built with FastAPI, SQLAlchemy, and modern Python best practices.

## Features

- **Database Management**: PostgreSQL with PostGIS support for geospatial data.
- **GeoServer Integration**: Full integration with GeoServer for geospatial services (Layers & Features).
    - **Dynamic BBOX Filtering**: Efficiently load only visible map features.
    - **Single-Item Retrieval**: Fetch individual layers and features by ID.
    - **WMS/WFS Support**: Direct integration with standard OGC services.
- **Time Series Processing**: Advanced time series data analysis and processing.
    - **Linkage**: Direct linking between Map Features and Water Stations (`feature.properties.station_id`).
    - **Metadata API**: List available time series data.
    - **Data API**: Retrieve, aggregate, and interpolate hydrological data.
- **RESTful API**: Comprehensive REST API with automatic documentation.
- **Data Models**: Strong typing with Pydantic V2 models (Schema) and SQLAlchemy ORM (DB).
- **Monitoring**: Built-in logging, metrics, and health checks.
- **Docker Support**: Complete containerization with Docker Compose.

## Architecture

```
water_dp/
├── app/
│   ├── api/                 # API endpoints (Geospatial, Time Series, Water Data)
│   │   └── v1/
│   │       └── endpoints/
│   ├── core/               # Core functionality (Config, DB, Logging, Seeding)
│   ├── models/             # SQLAlchemy Database models
│   ├── schemas/            # Pydantic validation schemas
│   ├── services/           # Business logic
│   │   ├── database_service.py    # CRUD for all entities
│   │   ├── geoserver_service.py   # GeoServer interaction
│   │   └── time_series_service.py # Analysis logic
│   └── main.py            # FastAPI application entry point
├── tests/                 # Unit and integration tests
│   ├── test_services/     # Service layer tests
│   └── conftest.py        # Test configuration
├── scripts/               # Utility scripts (Verification, Testing)
├── pyproject.toml         # Poetry dependencies and config
├── requirements.txt       # Legacy pip dependencies (if any)
├── docker-compose.yml     # Docker services orchestration
├── Dockerfile            # Application container definition
└── README.md             # This file
```

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
   # Copy environment template
   cp env.example .env
   # Edit .env if needed (defaults usually work for local dev)
   ```

3. **Start all services with Docker**
   ```bash
   docker-compose up -d --build
   ```
   This will start:
   - **Postgres (PostGIS)**: Database (Port 5432)
   - **Redis**: Caching (Port 6379)
   - **GeoServer**: Map Server (Port 8080)
   - **API**: Backend Application (Port 8000)

4. **Access the application**
   - **API Docs**: [http://localhost:8000/api/v1/docs](http://localhost:8000/api/v1/docs)
   - **GeoServer Admin**: [http://localhost:8080/geoserver](http://localhost:8080/geoserver) (Default: admin/geoserver)

### Verification
To verify that the entire stack is working correctly (Layers, Features, Data Linkage, and Time Series content), run the included verification script:

```bash
# Using poetry (recommended)
poetry run python scripts/verify_api.py

# Or using pipenv/venv
pip install requests
python scripts/verify_api.py
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
# Run all tests
poetry run pytest

# Run with coverage report
poetry run pytest --cov=app tests/
```

### Code Structure
- **Services**: Business logic is encapsulated in `app/services/`.
- **Schemas**: Pydantic V2 models (`ConfigDict`, `field_validator`) in `app/schemas/`.
- **Models**: SQLAlchemy models in `app/models/`.

## API Usage Guide

### 1. Geospatial Data
- **List Layers**: `GET /api/v1/geospatial/layers`
- **Get Features (with BBOX)**: `GET /api/v1/geospatial/features?layer_name=czech_regions&bbox=12.0,48.5,18.9,51.1`
- **Get Single Feature**: `GET /api/v1/geospatial/features/{feature_id}?layer_name=czech_regions`

### 2. Time Series Data
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
- [ ] Use TimeIO to replace the custom time-series storage engine

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
