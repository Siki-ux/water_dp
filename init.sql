-- Initialize Water Data Platform Database
-- This script sets up the database with PostGIS extension and initial configuration

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;

-- Create database user for the application (optional)
-- CREATE USER water_dp_user WITH PASSWORD 'water_dp_password';
-- GRANT ALL PRIVILEGES ON DATABASE water_data TO water_dp_user;

-- Set timezone
SET timezone = 'UTC';

-- Create initial schema if needed
-- CREATE SCHEMA IF NOT EXISTS water_data;

-- Grant permissions
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO postgres;
