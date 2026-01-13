import os
import sys

# Ensure app is in path
sys.path.append(os.getcwd())

from app.core.database import Base, engine


def sync_db():
    print("Creating all tables...")
    Base.metadata.create_all(bind=engine)
    print("Tables created (if they didn't exist).")


if __name__ == "__main__":
    sync_db()
