import asyncpg
import json
from typing import Optional, List, Any, Dict
import logging
from .config import get_settings

logger = logging.getLogger(__name__)


class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.settings = get_settings()

    async def get_pool(self) -> asyncpg.Pool:
        """Initialize and return the connection pool"""
        if not self.pool:
            database_url = self.settings.get_database_url()
            logger.info(f"Initializing database connection pool")
            try:
                async def _init_connection(conn: asyncpg.Connection) -> None:
                    await conn.set_type_codec(
                        "jsonb",
                        encoder=json.dumps,
                        decoder=json.loads,
                        schema="pg_catalog"
                    )
                    await conn.set_type_codec(
                        "json",
                        encoder=json.dumps,
                        decoder=json.loads,
                        schema="pg_catalog"
                    )

                self.pool = await asyncpg.create_pool(
                    database_url,
                    min_size=2,
                    max_size=10,
                    command_timeout=60,
                    init=_init_connection
                )
                logger.info("Database connection pool created successfully")
            except Exception as e:
                logger.error(f"Failed to create database pool: {e}")
                raise
        return self.pool

    async def execute(self, query: str, *args) -> str:
        """Execute a query and return the status"""
        pool = await self.get_pool()
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(query, *args)
                return result
        except Exception as e:
            logger.error(f"Database execute error: {e}")
            raise

    async def fetch(self, query: str, *args) -> List[Dict[str, Any]]:
        """Fetch multiple rows as dictionaries"""
        pool = await self.get_pool()
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(query, *args)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Database fetch error: {e}")
            raise

    async def fetchone(self, query: str, *args) -> Optional[Dict[str, Any]]:
        """Fetch a single row as a dictionary"""
        pool = await self.get_pool()
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, *args)
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"Database fetchone error: {e}")
            raise

    async def fetchval(self, query: str, *args) -> Any:
        """Fetch a single value"""
        pool = await self.get_pool()
        try:
            async with pool.acquire() as conn:
                return await conn.fetchval(query, *args)
        except Exception as e:
            logger.error(f"Database fetchval error: {e}")
            raise

    async def close(self):
        """Close the connection pool"""
        if self.pool:
            logger.info("Closing database connection pool")
            try:
                await self.pool.close()
            finally:
                self.pool = None
