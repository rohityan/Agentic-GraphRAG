"""Issue Ingestion Pipeline."""
import json
import logging
import asyncio
from datetime import datetime, timezone

from google.genai import types
from ingestion.core.settings import REPO_OWNER, REPO_NAME, FORCE_RESYNC, ADK_COMPONENTS
from ingestion.core.clients import ai_client, ai_semaphore, database
from ingestion.core.database import get_last_sync_time, update_sync_state, spanner_bulk_write
from ingestion.core.github import get_github_session, sync_repo_collaborators, categorize_labels
from ingestion.models import IssueSummaryResponse

logger = logging.getLogger("IssueSync")

def get_existing_issues_map():
    with database.snapshot() as s: return {row[0]: row[1] for row in s.execute_sql("SELECT issue_number, updated_at FROM Issues")}

async def fetch_incremental_issues(session, since_timestamp):
    filter_str = f", filterBy: {{since: \"{since_timestamp}\"}}" if since_timestamp else ""
    query = f"""
    query($owner: String!, $name: String!, $cursor: String) {{
      repository(owner: $owner, name: $name) {{
        issues(first: 50, after: $cursor{filter_str}, orderBy: {{field: UPDATED_AT, direction: ASC}}) {{
          pageInfo {{ hasNextPage, endCursor }}
          nodes {{ id number title state createdAt updatedAt closedAt body authorAssociation reactions(content: THUMBS_UP) {{ totalCount }} author {{ login ... on User {{ company followers {{ totalCount }} }} }} assignees(first: 5) {{ nodes {{ login }} }} labels(first: 10) {{ nodes {{ name }} }} comments(last: 50) {{ nodes {{ id author {{ login }} createdAt body }} }} }}
        }}
      }}
    }}
    """
    all_issues, cursor =[], None
    while True:
        async with session.post("https://api.github.com/graphql", json={"query": query, "variables": {"owner": REPO_OWNER, "name": REPO_NAME, "cursor": cursor}}) as resp:
            resp.raise_for_status()
            data = (await resp.json())["data"]["repository"]["issues"]
            all_issues.extend(data.get("nodes", []))
            if not data["pageInfo"]["hasNextPage"]: break
            cursor = data["pageInfo"]["endCursor"]
    return all_issues

async def process_issue(issue, idx, total):
    async with ai_semaphore:
        issue_num = int(issue["number"])
        logger.info(f"[{idx}/{total}] Processing Issue #{issue_num}...")
        
        c_str, s_str, o_str = categorize_labels(issue.get("labels", {}).get("nodes",[]), ADK_COMPONENTS)
        auth = issue.get("author") or {}
        author_name = auth.get("login", "Ghost")
        raw_body = issue.get("body", "") or "No description."
        
        chronology, comment_rows = [],[]
        for c in issue.get("comments", {}).get("nodes",[]):
            c_auth = c.get("author", {}).get("login", "Ghost") if c.get("author") else "Ghost"
            chronology.append(f"[{c.get('createdAt', '')[:10]}] @{c_auth}: {c.get('body', '')[:500]}")
            if "id" in c: comment_rows.append((issue_num, c["id"], c_auth, c.get("createdAt", "")))
            
        try:
            # 🚀 THE FIX: Restored your strict Triage and Resolution rules!
            prompt = f"""
            Analyze Issue: {issue.get('title')}
            State: {issue.get('state')}
            Body: {raw_body[:3000]}
            
            === DISCUSSION TIMELINE ===
            {chr(10).join(chronology)}
            
            === TRIAGE RULES ===
            Based strictly on the timeline above, assign the `triage_category`:
            - If State is CLOSED, output: "Resolved"
            - Category 1: "Missing Maintainer Response" (No maintainer has replied yet).
            - Category 2: "Waiting on User" (A maintainer asked a question and the user has NOT fulfilled it yet).
            - Category 3: "Waiting on Maintainer" (The user replied, and is now waiting for a maintainer).
            - Category 4: "Needs Internal Review" (A maintainer tagged another maintainer).
            
            === RESOLUTION DETECTION ===
            Analyze the timeline to see if the issue was solved. Set `has_maintainer_resolution` to true ONLY IF a maintainer explicitly provided:
            1. A code workaround or configuration fix.
            2. Confirmation of a merged PR that fixes this.
            3. A definitive statement like "working as intended" or "won't fix".
            If true, put the exact instructions/quote in `resolution_snippet`.
            """
            
            resp = await ai_client.aio.models.generate_content(
                model='gemini-2.5-flash', contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=IssueSummaryResponse)
            )
            ai_data = IssueSummaryResponse.model_validate_json(resp.text)
            vector = (await ai_client.aio.models.embed_content(model='text-embedding-004', contents=f"Title: {issue.get('title')}\nSummary:\n{ai_data.issue_summary}")).embeddings[0].values
        except Exception as e:
            logger.warning(f"⚠️ AI Failed for Issue #{issue_num}: {e}")
            ai_data = IssueSummaryResponse(issue_type="Unknown", issue_summary="AI Failed", triage_category="Unknown", has_maintainer_resolution=False, resolution_snippet="None", environment_versions={})
            vector = None

        issue_row = (
            issue_num, issue["id"], issue.get("title", ""), author_name, issue["state"], 
            ", ".join([a["login"] for a in issue.get("assignees", {}).get("nodes",[])]), 
            c_str, s_str, o_str, ai_data.issue_type, issue.get("createdAt"), issue["updatedAt"], issue.get("closedAt"), 
            raw_body[:30000], ai_data.issue_summary, json.dumps(ai_data.environment_versions), 
            issue.get("reactions", {}).get("totalCount", 0), issue.get("authorAssociation", "NONE"), vector, 
            ai_data.triage_category, ai_data.has_maintainer_resolution, ai_data.resolution_snippet
        )
        user_row = (author_name, auth.get("company", "None"), auth.get("followers", {}).get("totalCount", 0), "Contributor", datetime.now(timezone.utc).isoformat()) if author_name != "Ghost" else None
        
        return {"issue_row": issue_row, "comment_rows": comment_rows, "user_row": user_row, "updated_at": issue["updatedAt"]}
async def main():
    logger.info(f"Starting Issue Sync... (FORCE_RESYNC={FORCE_RESYNC})")
    last_sync = await asyncio.to_thread(get_last_sync_time, "issues_sync")
    existing_map = await asyncio.to_thread(get_existing_issues_map)

    async with get_github_session(is_graphql=True) as session:
        await sync_repo_collaborators(session)
        issues = await fetch_incremental_issues(session, last_sync)
        if not issues: 
            logger.info("✅ No new issues found. Already up-to-date.")
            return

        tasks =[process_issue(iss, i, len(issues)) for i, iss in enumerate(issues, 1) if FORCE_RESYNC or existing_map.get(int(iss["number"])) != iss["updatedAt"]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    i_rows, c_rows, u_map, valid_ts = [], [], {},[]
    for res in results:
        if res and not isinstance(res, Exception):
            i_rows.append(res["issue_row"])
            c_rows.extend(res["comment_rows"])
            valid_ts.append(res["updated_at"])
            if res["user_row"]: u_map[res["user_row"][0]] = res["user_row"]

    await asyncio.to_thread(spanner_bulk_write, "Users", ("github_handle", "company", "followers", "system_role", "last_updated"), list(u_map.values()), 2000)
    await asyncio.to_thread(spanner_bulk_write, "Issues", ("issue_number", "graphql_id", "title", "author", "state", "assignees", "components", "statuses", "other_labels", "issue_type", "created_at", "updated_at", "closed_at", "body_text", "issue_summary", "environment_versions", "upvotes", "author_association", "embedding", "triage_category", "has_maintainer_resolution", "resolution_snippet"), i_rows, 500)
    await asyncio.to_thread(spanner_bulk_write, "IssueComments", ("issue_number", "comment_id", "author", "created_at"), c_rows, 2000)

    if valid_ts: await asyncio.to_thread(update_sync_state, "issues_sync", max(valid_ts))
    await asyncio.sleep(0.1)
    logger.info("✅ Issue Sync Complete.")

if __name__ == "__main__": asyncio.run(main())