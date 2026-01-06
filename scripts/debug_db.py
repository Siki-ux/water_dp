import logging

from sqlalchemy import text

from app.core.database import SessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_tables():
    db = SessionLocal()
    try:
        # queries for public schema tables
        result = db.execute(
            text(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
        )
        tables = [row[0] for row in result]
        print("Tables in public schema:")
        for t in sorted(tables):
            print(f" - {t}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    check_tables()
