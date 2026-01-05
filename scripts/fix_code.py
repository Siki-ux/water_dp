#!/usr/bin/env python3
"""
Script to automatically fix linting and formatting issues.
"""
import logging
import subprocess
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def run_command(command: str, description: str) -> bool:
    """Run a shell command and return success status."""
    try:
        logger.info(f"Running: {description}")
        # Use shell=True to pick up path provided by poetry shell or venv
        subprocess.run(command, shell=True, check=True)
        logger.info(f"Success: {description}")
        return True
    except subprocess.CalledProcessError:
        logger.error(f"Failed: {description}")
        return False


def main():
    """Run fix commands."""
    logger.info("Starting code auto-fix...")

    success = True

    # 1. Run Ruff with --fix
    # This handles unused imports, unused variables, sorting imports, etc.
    if not run_command("ruff check --fix .", "Ruff (Linting Fixes)"):
        logger.warning(
            "Ruff found issues it could not fix automatically (or reported remaining errors)."
        )
        # specific ruff failures (like remaining errors) might exit non-zero, catch that?
        # ruff check exits non-zero if violations remain. We should continue to formatting.
        # ruff check exits non-zero if violations remain. We should continue to formatting.
        # pass statement is unnecessary here since comments handle the block body if empty, or just continue.
        # Actually empty block needs pass, but the if body has logging warning.
        # Wait, the code is:
        # if not run_command(...):
        #    logger.warning(...)
        #    pass
        # simpler: just remove pass.

    # 2. Run Black
    # This handles code formatting (line length, spacing, etc.)
    if not run_command("black .", "Black (Code Formatting)"):
        success = False

    if success:
        logger.info("Code auto-fix completed.")
    else:
        logger.error("Auto-fix encountered errors.")
        sys.exit(1)


if __name__ == "__main__":
    main()
