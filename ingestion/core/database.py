"""Shared Spanner Read/Write operations."""
import logging
from typing import List, Tuple, Optional
from .clients import database
from .settings import FORCE_RESYNC

logger = logging.getLogger("DatabaseCore")

def get_last_sync_time(job_name: str) -> Optional[str]:
    """Retrieves the timestamp of the last successful sync."""
    if FORCE_RESYNC: 
        return None
    with database.snapshot() as snapshot:
        for row in snapshot.execute_sql(f"SELECT last_value FROM SyncState WHERE job_name = '{job_name}'"): 
            return row[0]
    return None

def update_sync_state(job_name: str, last_updated: str) -> None:
    """Updates the sync cursor watermark."""
    with database.batch() as batch:
        batch.insert_or_update(
            table="SyncState", columns=("job_name", "last_value"), values=[(job_name, last_updated)]
        )

def spanner_bulk_write(table: str, columns: Tuple[str, ...], rows: List[Tuple], chunk_size: int = 500) -> None:
    """Generic batched writer that respects Spanner's mutation limits."""
    if not rows: return
    for i in range(0, len(rows), chunk_size):
        logger.info(f"Committing {table} batch {i // chunk_size + 1}...")
        with database.batch() as batch:
            batch.insert_or_update(table=table, columns=columns, values=rows[i:i+chunk_size])