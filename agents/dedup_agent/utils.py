import logging
import asyncio
from typing import List, Dict
from google.cloud import spanner
from .settings import GCP_PROJECT, SPANNER_INSTANCE, SPANNER_DATABASE, FORCE_RESYNC

logger = logging.getLogger(__name__)

spanner_client = spanner.Client(project=GCP_PROJECT)
database = spanner_client.instance(SPANNER_INSTANCE).database(SPANNER_DATABASE)

_API_CALLS = 0
def get_api_call_count() -> int: return _API_CALLS
def reset_api_call_count() -> None: global _API_CALLS; _API_CALLS = 0
def increment_api_call_count() -> None: global _API_CALLS; _API_CALLS += 1

async def get_records_to_process(table: str, id_col: str) -> List[Dict]:
    """Fetches records asynchronously without blocking the event loop."""
    where_clause = "1=1" if FORCE_RESYNC else "ai_duplicate_analysis IS NULL"
    sql = f"SELECT {id_col} as id, title, issue_summary as summary FROM {table} WHERE embedding IS NOT NULL AND {where_clause}"
    if table == "PullRequests":
        sql = sql.replace("issue_summary", "pr_summary")
        
    def _fetch():
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(sql)
            increment_api_call_count()
            return [{"id": row[0], "title": row[1], "summary": row[2]} for row in results]
            
    return await asyncio.to_thread(_fetch)

async def save_suggestion_to_spanner(table: str, id_col: str, record_id: int, suggestion: str, verified_ids: str) -> None:
    """Updates a single record in Spanner asynchronously."""
    def _save():
        with database.batch() as batch:
            batch.insert_or_update(
                table=table, 
                columns=(id_col, "ai_duplicate_analysis", "verified_duplicate_ids"), 
                values=[(record_id, suggestion, verified_ids)]
            )
        increment_api_call_count()
        
    await asyncio.to_thread(_save)