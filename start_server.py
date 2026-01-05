#!/usr/bin/env python3
"""
Start the Water Data Platform server with Swagger documentation.
"""
import sys
import time
import webbrowser
from pathlib import Path

import uvicorn

# Add the app directory to the Python path
sys.path.append(str(Path(__file__).parent))

from app.core.config import settings


def start_server():
    """Start the server and open Swagger documentation."""
    print("Starting Water Data Platform API...")
    print(
        f"Swagger UI will be available at: http://localhost:8000{settings.api_prefix}/docs"
    )
    print(
        f"ReDoc will be available at: http://localhost:8000{settings.api_prefix}/redoc"
    )
    print(f"OpenAPI JSON: http://localhost:8000{settings.api_prefix}/openapi.json")
    print("\n" + "=" * 60)
    print("Opening Swagger UI in your browser...")
    print("=" * 60 + "\n")

    # Open browser after a short delay
    def open_browser():
        time.sleep(2)
        webbrowser.open(f"http://localhost:8000{settings.api_prefix}/docs")

    import threading

    browser_thread = threading.Thread(target=open_browser)
    browser_thread.daemon = True
    browser_thread.start()

    # Start the server
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
        access_log=True,
    )


if __name__ == "__main__":
    start_server()
