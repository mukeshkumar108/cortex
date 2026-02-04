from pathlib import Path
import logging
from typing import List
from .db import Database

logger = logging.getLogger(__name__)


async def run_migrations(db: Database) -> None:
    """
    Execute SQL migrations in order and record applied migrations.
    """
    await _ensure_migrations_table(db)

    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    if not migrations_dir.exists():
        logger.warning("Migrations directory not found; skipping migrations")
        return

    migration_files = sorted(p for p in migrations_dir.glob("*.sql"))
    if not migration_files:
        logger.info("No migrations found")
        return

    applied = await _get_applied_migrations(db)

    for path in migration_files:
        name = path.name
        if name in applied:
            continue

        sql = path.read_text()
        logger.info(f"Applying migration {name}")
        await db.execute(sql)
        await db.execute(
            "INSERT INTO schema_migrations (name) VALUES ($1)",
            name
        )


async def _ensure_migrations_table(db: Database) -> None:
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
        """
    )


async def _get_applied_migrations(db: Database) -> List[str]:
    rows = await db.fetch("SELECT name FROM schema_migrations ORDER BY applied_at ASC")
    return [row["name"] for row in rows]
