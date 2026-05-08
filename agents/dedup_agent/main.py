import re
import time
import asyncio
import logging
from typing import Tuple, Dict

from google.adk.cli.utils import logs
from google.adk.runners import InMemoryRunner
from google.genai import types

from .settings import CONCURRENCY_LIMIT, SLEEP_BETWEEN_CHUNKS, APP_NAME, USER_ID
from .utils import get_records_to_process, save_suggestion_to_spanner, get_api_call_count, reset_api_call_count
from .agent import dedup_agent, AgenticAnalysisResult

logs.setup_adk_logger(level=logging.INFO)
logger = logging.getLogger("google_adk." + __name__)

async def process_single_record(record: Dict, table_name: str, id_col: str) -> Tuple[float, int, bool]:
    start_time = time.perf_counter()
    start_api_calls = get_api_call_count()
    record_id = record["id"]
    is_success = False

    logger.info(f"Processing {table_name} #{record_id}...")

    try:
        runner = InMemoryRunner(agent=dedup_agent, app_name=APP_NAME)
        session = await runner.session_service.create_session(user_id=USER_ID, app_name=APP_NAME)

        prompt_text = f"Analyze {table_name} #{record_id}.\nTitle: {record['title']}\nSummary: {record['summary']}"
        prompt_message = types.Content(role="user", parts=[types.Part(text=prompt_text)])

        full_response = ""
        async for event in runner.run_async(user_id=USER_ID, session_id=session.id, new_message=prompt_message):
            if event.content and event.content.parts and hasattr(event.content.parts[0], "text"):
                text = event.content.parts[0].text
                if text:
                    full_response += text
        
        if full_response:
            json_match = re.search(r'\{.*\}', full_response, re.DOTALL)
            if not json_match:
                raise ValueError(f"No JSON object found in Agent response: {full_response[:100]}...")
                
            clean_resp = json_match.group(0)
            ai_data = AgenticAnalysisResult.model_validate_json(clean_resp)
            
            verified_ids_str = ", ".join(map(str, ai_data.verified_duplicate_ids)) if ai_data.verified_duplicate_ids else "None"
            suggestion_text = ai_data.ai_duplicate_analysis
            
            await save_suggestion_to_spanner(table_name, id_col, record_id, suggestion_text, verified_ids_str)
            logger.info(f"#{record_id} Successfully saved AI Analysis.")
            is_success = True

    except Exception as e:
        logger.error(f"Error processing #{record_id}: {e}")
        try:
            fallback_msg = f"Agent failed: {str(e)[:150]}"
            await save_suggestion_to_spanner(table_name, id_col, record_id, fallback_msg, "None")
        except Exception as db_err:
            logger.error(f"Could not save fallback for #{record_id}: {db_err}")

    duration = time.perf_counter() - start_time
    issue_api_calls = get_api_call_count() - start_api_calls

    return duration, issue_api_calls, is_success


async def process_table(table_name: str, id_col: str) -> Tuple[int, int]:
    logger.info(f"--- Fetching {table_name} to review ---")
    records = await get_records_to_process(table_name, id_col)

    total_count = len(records)
    if total_count == 0:
        logger.info(f"No {table_name} pending analysis.")
        return 0, 0

    logger.info(f"Found {total_count} {table_name} to process.")
    
    success_count = 0
    failure_count = 0
    processed_count = 0

    for i in range(0, total_count, CONCURRENCY_LIMIT):
        chunk = records[i : i + CONCURRENCY_LIMIT]
        current_chunk_num = i // CONCURRENCY_LIMIT + 1

        logger.info(f"Starting chunk {current_chunk_num}: {[r['id'] for r in chunk]}")

        tasks =[process_single_record(record, table_name, id_col) for record in chunk]
        results = await asyncio.gather(*tasks)

        for _, _, is_success in results:
            if is_success:
                success_count += 1
            else:
                failure_count += 1

        processed_count += len(chunk)
        
        if (i + CONCURRENCY_LIMIT) < total_count:
            await asyncio.sleep(SLEEP_BETWEEN_CHUNKS)

    return success_count, failure_count


async def main():
    logger.info("--- Starting ADK Deduplication Agent ---")
    reset_api_call_count()

    i_success, i_fail = await process_table("Issues", "issue_number")
    pr_success, pr_fail = await process_table("PullRequests", "pr_number")

    total_success = i_success + pr_success
    total_fail = i_fail + pr_fail

    logger.info("==================================================")
    logger.info(f"✅ Total Succeeded: {total_success}")
    logger.info(f"❌ Total Failed:    {total_fail}")
    logger.info("==================================================")


if __name__ == "__main__":
    start_time = time.perf_counter()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Bot execution interrupted manually.")
    except Exception as e:
        logger.critical(f"Unexpected fatal error: {e}", exc_info=True)

    duration = time.perf_counter() - start_time
    logger.info(f"Full execution finished in {duration/60:.2f} minutes.")