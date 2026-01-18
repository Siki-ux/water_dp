FROM flyway/flyway:10

COPY alembic/versions /flyway/sql
