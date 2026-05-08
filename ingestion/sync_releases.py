"""Releases Ingestion Pipeline (Feature Tracking)."""
import re
import logging
import asyncio
import hashlib

from google.genai import types
from ingestion.core.settings import REPO_OWNER, REPO_NAME, FORCE_RESYNC
from ingestion.core.clients import ai_client, ai_semaphore, database
from ingestion.core.database import get_last_sync_time, update_sync_state, spanner_bulk_write
from ingestion.core.github import get_github_session
from ingestion.models import FeatureSummary

from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log

logger = logging.getLogger("ReleaseSync")
release_semaphore = asyncio.Semaphore(3)
github_semaphore = asyncio.Semaphore(5)

def get_existing_releases_map():
    with database.snapshot() as s: 
        return {row[0]: row[1] for row in s.execute_sql("SELECT tag_name, published_at FROM Releases")}

async def fetch_releases(session) -> list:
    url, all_releases = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases?per_page=100",[]
    while url:
        async with session.get(url) as resp:
            resp.raise_for_status()
            all_releases.extend(await resp.json())
            url = resp.links.get("next", {}).get("url")
    return sorted(all_releases, key=lambda x: x.get("published_at", ""))

async def get_commit_diff(session, commit_hash: str) -> str:
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/commits/{commit_hash}"
    async with github_semaphore:
        async with session.get(url, headers={"Accept": "application/vnd.github.v3.diff"}) as resp:
            return await resp.text() if resp.status == 200 else ""

# 🚀 THE FIX: A dedicated AI helper that catches 429s and patiently waits instead of failing!
@retry(
    wait=wait_exponential(multiplier=2, min=5, max=60), 
    stop=stop_after_attempt(6),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
async def generate_feature_ai(prompt: str, line: str):
    async with ai_semaphore:
        resp = await ai_client.aio.models.generate_content(
            model='gemini-2.5-flash', contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=FeatureSummary)
        )
        feature_summary = FeatureSummary.model_validate_json(resp.text).summary
        feature_vec = (await ai_client.aio.models.embed_content(
            model='text-embedding-004', contents=f"Feature: {line}\nSummary: {feature_summary}"
        )).embeddings[0].values
        
        await asyncio.sleep(1.0) # Standard quota buffer
        return feature_summary, feature_vec

async def process_feature(session, tag_name: str, line: str):
    pr_mappings =[(tag_name, int(pr_num)) for pr_num in set(re.findall(r'#(\d+)', line))]
    commit_match = re.search(r'\(([a-fA-F0-9]{7,40})\)', line)
    commit_hash = commit_match.group(1) if commit_match else None
    diff_text = await get_commit_diff(session, commit_hash) if commit_hash else ""
    
    prompt = f"Analyze this release feature:\nDesc: {line}\nDiff:\n{diff_text[:15000]}"
    
    try:
        # Use our new resilient AI helper
        feature_summary, feature_vec = await generate_feature_ai(prompt, line)
    except Exception as e:
        logger.error(f"❌ AI Ultimately Failed for feature in {tag_name} after all retries: {e}")
        feature_summary, feature_vec = "AI Skipped.", None

    return {
        "pr_mappings": pr_mappings,
        "feature_row": (tag_name, hashlib.md5(f"{tag_name}_{line}".encode()).hexdigest(), line[:5000], commit_hash, diff_text[:15000], feature_summary, feature_vec)
    }

async def process_release(session, release, idx, total):
    async with release_semaphore:
        tag_name = release["tag_name"]
        logger.info(f"[{idx}/{total}] Processing Release: {tag_name}...")
        body_text = release.get("body", "") or "No release notes provided."
        
        try:
            async with ai_semaphore:
                main_vec = (await ai_client.aio.models.embed_content(model='text-embedding-004', contents=f"Release {tag_name}\n\n{body_text[:8000]}")).embeddings[0].values
                await asyncio.sleep(1.0)
        except Exception: 
            main_vec = None

        release_row = (tag_name, release.get("name", tag_name), release.get("published_at", ""), body_text, "Chunked", main_vec)
        
        tasks =[process_feature(session, tag_name, line.strip()) for line in body_text.split('\n') if line.strip() and not line.strip().startswith('#') and len(line.strip()) > 10]
        
        # 🚀 THE FIX: Process massive releases in chunks so we don't bombard the API at the exact same second
        f_rows, pr_maps = [],[]
        chunk_size = 5
        for i in range(0, len(tasks), chunk_size):
            chunk_tasks = tasks[i:i+chunk_size]
            f_res = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            for res in f_res:
                if res and not isinstance(res, Exception):
                    f_rows.append(res["feature_row"])
                    pr_maps.extend(res["pr_mappings"])
                
        return {"release_row": release_row, "feature_rows": f_rows, "pr_mappings": pr_maps, "published_at": release.get("published_at")}

async def main():
    logger.info(f"Starting Release Sync... (FORCE_RESYNC={FORCE_RESYNC})")
    last_sync = await asyncio.to_thread(get_last_sync_time, "releases_sync")
    existing_map = await asyncio.to_thread(get_existing_releases_map)

    async with get_github_session(is_graphql=False) as session:
        releases = await fetch_releases(session)
        if last_sync and not FORCE_RESYNC: releases =[r for r in releases if r.get("published_at", "") > last_sync]
        if not releases:
            logger.info("✅ No new releases found.")
            return

        tasks =[process_release(session, r, i, len(releases)) for i, r in enumerate(releases, 1) if FORCE_RESYNC or existing_map.get(r["tag_name"]) != r["published_at"]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    r_rows, f_rows, pr_maps, valid_ts = [], [], [],[]
    for res in results:
        if res and not isinstance(res, Exception):
            r_rows.append(res["release_row"])
            f_rows.extend(res["feature_rows"])
            pr_maps.extend(res["pr_mappings"])
            valid_ts.append(res["published_at"])

    await asyncio.to_thread(spanner_bulk_write, "Releases", ("tag_name", "name", "published_at", "body_text", "diff_text", "embedding"), r_rows, 500)
    await asyncio.to_thread(spanner_bulk_write, "ReleaseFeatures", ("tag_name", "feature_id", "description", "commit_hash", "diff_text", "summary", "embedding"), f_rows, 1000)
    await asyncio.to_thread(spanner_bulk_write, "ReleaseIncludesPR", ("release_tag", "pr_number"), pr_maps, 2000)

    if valid_ts: await asyncio.to_thread(update_sync_state, "releases_sync", max(valid_ts))
    await asyncio.sleep(0.1)
    logger.info("✅ Release Sync Complete.")

if __name__ == "__main__": asyncio.run(main())