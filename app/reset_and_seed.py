import logging

from app.core.database import Base, SessionLocal, engine, init_db
from app.core.seeding import seed_data

# Configure logging to show info
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add create_geo_feature path trick if checking from script directly,
# but running as module (python -m app.reset_and_seed) is better.


def reset_db():
    logger.info("Dropping all tables...")
    Base.metadata.drop_all(bind=engine)
    logger.info("Tables dropped.")

    logger.info("Initializing DB...")
    init_db()
    logger.info("DB Initialized.")

    logger.info("Seeding data...")
    db = SessionLocal()
    try:
        seed_data(db)
    finally:
        db.close()
    logger.info("Done.")


if __name__ == "__main__":
    reset_db()
