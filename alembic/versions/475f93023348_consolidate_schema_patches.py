"""consolidate_schema_patches

Revision ID: 475f93023348
Revises: a1b2c3d4e5f6
Create Date: 2026-01-26 09:29:08.193790

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "475f93023348"
down_revision = "a1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Create simulations table
    op.execute(
        """
    CREATE TABLE IF NOT EXISTS simulations (
        id VARCHAR PRIMARY KEY,
        thing_uuid VARCHAR NOT NULL UNIQUE,
        config JSONB NOT NULL,
        is_enabled BOOLEAN DEFAULT TRUE,
        last_run TIMESTAMPTZ,
        interval_seconds INTEGER DEFAULT 60,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ
    );
    """
    )
    op.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_simulations_thing_uuid ON simulations(thing_uuid);
    """
    )

    # 2. Fix projects schema (add authorized_provider_group_id)
    op.execute(
        """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name='projects'
            AND column_name='authorization_provider_group_id'
        ) THEN
            ALTER TABLE projects ADD COLUMN authorization_provider_group_id VARCHAR(255);
            CREATE INDEX IF NOT EXISTS idx_projects_authorization_provider_group_id ON projects(authorization_provider_group_id);
        END IF;
    END $$;
    """
    )

    # 3. Fix simulations columns
    op.execute(
        """
    DO $$
    BEGIN
        -- Add created_by
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='simulations' AND column_name='created_by') THEN
            ALTER TABLE simulations ADD COLUMN created_by VARCHAR(100);
        END IF;

        -- Add updated_by
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='simulations' AND column_name='updated_by') THEN
            ALTER TABLE simulations ADD COLUMN updated_by VARCHAR(100);
        END IF;

        -- Add metadata_json
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='simulations' AND column_name='metadata_json') THEN
            ALTER TABLE simulations ADD COLUMN metadata_json TEXT;
        END IF;
    END $$;
    """
    )


def downgrade() -> None:
    pass
