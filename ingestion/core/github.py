"""Shared GitHub networking and data extraction logic."""
import logging
import asyncio
import aiohttp
from datetime import datetime, timezone
from typing import List, Dict, Tuple
from tenacity import retry, wait_exponential, stop_after_attempt, before_sleep_log
from .settings import GITHUB_TOKEN, REPO_OWNER, REPO_NAME
from .database import spanner_bulk_write

logger = logging.getLogger("GitHubCore")

def get_github_session(is_graphql: bool = False) -> aiohttp.ClientSession:
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}", "Accept": "application/vnd.github.v3+json"}
    if is_graphql: headers["X-GitHub-Api-Version"] = "2022-11-28"
    return aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=60))

@retry(wait=wait_exponential(multiplier=2, max=60), stop=stop_after_attempt(5), before_sleep=before_sleep_log(logger, logging.WARNING))
async def sync_repo_collaborators(session: aiohttp.ClientSession) -> None:
    """Fetches all repository collaborators and standardizes their roles."""
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/collaborators"
    params, all_collaborators = {"per_page": "100"},[]
    
    while url:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status() 
            data = await resp.json()
            if not isinstance(data, list): return
            all_collaborators.extend(data)
            url, params = (str(resp.links["next"]["url"]), None) if "next" in resp.links else (None, None)

    now_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    users_mutations = [(u["login"], "Unknown", 0, "Maintainer", now_str) for u in all_collaborators if "login" in u]
    if users_mutations:
        await asyncio.to_thread(spanner_bulk_write, "Users", ("github_handle", "company", "followers", "system_role", "last_updated"), users_mutations, 2000)
        logger.info(f"✅ Synced {len(users_mutations)} maintainers.")

def categorize_labels(label_nodes: List[Dict[str, str]], adk_components: set) -> Tuple[str, str, str]:
    components, statuses, others = [], [],[]
    for node in label_nodes:
        label, label_lower = node.get("name", ""), node.get("name", "").lower()
        if label_lower in adk_components: components.append(label)
        elif label_lower.startswith(("status", "resolution", "state")): statuses.append(label)
        else: others.append(label)
    return (", ".join(components) or "None", ", ".join(statuses) or "None", ", ".join(others) or "None")