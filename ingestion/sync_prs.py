"""Pull Request Ingestion Pipeline."""
import logging
import asyncio
import hashlib
from datetime import datetime, timezone

from google.genai import types
from ingestion.core.settings import REPO_OWNER, REPO_NAME, FORCE_RESYNC, ADK_COMPONENTS
from ingestion.core.clients import ai_client, ai_semaphore, database
from ingestion.core.database import get_last_sync_time, update_sync_state, spanner_bulk_write
from ingestion.core.github import get_github_session, sync_repo_collaborators, categorize_labels
from ingestion.models import PRSummaryResponse

logger = logging.getLogger("PRSync")

def get_existing_prs_map():
    with database.snapshot() as s: 
        return {row[0]: row[1] for row in s.execute_sql("SELECT pr_number, updated_at FROM PullRequests")}

async def get_git_diff(session, pr_number: int) -> str:
    url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/pulls/{pr_number}"
    async with session.get(url, headers={"Accept": "application/vnd.github.v3.diff"}) as resp:
        return await resp.text() if resp.status == 200 else ""

def chunk_git_diff(raw_diff: str):
    chunks =[]
    for part in raw_diff.split('diff --git '):
        if not part.strip(): continue
        first_line = part.split('\n', 1)[0]
        file_path = first_line.split()[-1].replace('b/', '') if 'b/' in first_line else first_line
        chunks.append((file_path, part[:8000]))
    return chunks

def extract_python_modules(file_paths: list) -> str:
    modules = set([p[4:-3].replace('/', '.') if p.startswith('src/') and p.endswith('.py') else p for p in file_paths])
    return ", ".join(sorted(list(modules))) if modules else "None"

async def fetch_incremental_prs(session, since_timestamp):
    query = """
    query($owner: String!, $name: String!, $cursor: String) {
      repository(owner: $owner, name: $name) {
        pullRequests(first: 50, after: $cursor, orderBy: {field: UPDATED_AT, direction: DESC}) {
          pageInfo { hasNextPage endCursor }
          nodes { number title body state isDraft updatedAt createdAt url authorAssociation reactions(content: THUMBS_UP) { totalCount } author { login ... on User { company followers { totalCount } } } assignees(first: 5) { nodes { login } } labels(first: 10) { nodes { name } } closingIssuesReferences(first: 10) { nodes { number } } comments(last: 50) { nodes { id author { login } createdAt body } } reviews(first: 10) { nodes { author { login } state } } commits(last: 1) { nodes { commit { statusCheckRollup { state } } } } }
        }
      }
    }
    """
    all_prs, cursor, has_next = {}, None, True
    while has_next:
        async with session.post("https://api.github.com/graphql", json={"query": query, "variables": {"owner": REPO_OWNER, "name": REPO_NAME, "cursor": cursor}}) as resp:
            resp.raise_for_status()
            data = (await resp.json())["data"]["repository"]["pullRequests"]
            for node in data.get("nodes", []):
                if since_timestamp and node["updatedAt"] < since_timestamp:
                    has_next = False
                    break
                all_prs[node["number"]] = node
            if has_next:
                has_next = data["pageInfo"]["hasNextPage"]
                cursor = data["pageInfo"]["endCursor"]
    return sorted(list(all_prs.values()), key=lambda x: x["updatedAt"])

async def process_pr(session, pr, idx, total):
    async with ai_semaphore:
        pr_num = pr["number"]
        logger.info(f"[{idx}/{total}] Processing PR #{pr_num}...")
        
        pr_state = "DRAFT" if pr.get("isDraft") else pr.get("state", "UNKNOWN")
        raw_diff = await get_git_diff(session, pr_num)
        diff_chunks = chunk_git_diff(raw_diff)
        
        auth = pr.get("author") or {}
        author_name = auth.get("login", "Ghost")
        raw_body = pr.get("body", "") or "No description."
        
        chronology, comment_rows = [],[]
        for c in pr.get("comments", {}).get("nodes",[]):
            c_auth = c.get("author", {}).get("login", "Ghost") if c.get("author") else "Ghost"
            chronology.append(f"[{c.get('createdAt', '')[:10]}] @{c_auth}: {c.get('body', '')[:500]}")
            if "id" in c: comment_rows.append((pr_num, c["id"], c_auth, c.get("createdAt", "")))
        
        diff_rows =[]
        try:
            # 🚀 THE FIX: Restored your strict Triage rules!
            prompt = f"""
            Analyze PR Diff for {pr.get('title')}
            State: {pr_state}
            Desc: {raw_body[:3000]}
            
            === DISCUSSION TIMELINE ===
            {chr(10).join(chronology)}
            
            === DIFF EXCERPT ===
            {raw_diff[:20000]}
            
            === TRIAGE RULES ===
            Based strictly on the timeline, assign the `triage_category`:
            - If State is MERGED or CLOSED, output: "Resolved"
            - Category 1: "Missing Maintainer Response" (No maintainer has reviewed or replied yet).
            - Category 2: "Waiting on User" (A maintainer requested changes or info, and the author hasn't provided them).
            - Category 3: "Waiting on Maintainer" (The author pushed new commits or replied, waiting for a maintainer).
            - Category 4: "Needs Internal Review" (A maintainer tagged another maintainer for a second opinion).
            """
            
            resp = await ai_client.aio.models.generate_content(
                model='gemini-2.5-flash', contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=PRSummaryResponse)
            )
            ai_data = PRSummaryResponse.model_validate_json(resp.text)
            main_vec = (await ai_client.aio.models.embed_content(model='text-embedding-004', contents=f"Title: {pr.get('title')}\nSummary:\n{ai_data.pr_summary}")).embeddings[0].values
            
            if diff_chunks:
                capped = diff_chunks[:50]
                vec_resp = await ai_client.aio.models.embed_content(model='text-embedding-004', contents=[f"File: {p}\nDiff:\n{d}" for p, d in capped])
                for i, (filepath, diff_text) in enumerate(capped):
                    diff_rows.append((pr_num, hashlib.md5(f"{pr_num}-{filepath}".encode()).hexdigest(), filepath, diff_text, vec_resp.embeddings[i].values))
        except Exception as e:
            logger.warning(f"⚠️ AI Failed for PR #{pr_num}: {e}")
            ai_data = PRSummaryResponse(pr_type="Unknown", pr_summary="AI failed.", modified_symbols=[], triage_category="Unknown")
            main_vec = None

        c_str, s_str, o_str = categorize_labels(pr.get("labels", {}).get("nodes",[]), ADK_COMPONENTS)
        commits = pr.get("commits", {}).get("nodes",[])
        ci_status = commits[0]["commit"]["statusCheckRollup"].get("state", "UNKNOWN") if commits and commits[0].get("commit", {}).get("statusCheckRollup") else "UNKNOWN"
        reviews = pr.get("reviews", {}).get("nodes",[])
        
        pr_row = (
            pr_num, pr.get("title", ""), author_name, ", ".join([a["login"] for a in pr.get("assignees", {}).get("nodes",[])]), 
            ", ".join(list(set([r.get("author", {}).get("login", "Ghost") for r in reviews if r.get("author")]))) or "None", 
            pr_state, pr.get("url", ""), ai_data.pr_type, pr.get("createdAt", ""), pr["updatedAt"], 
            c_str, s_str, o_str, raw_body[:30000], ai_data.pr_summary, raw_diff[:10000], ci_status, 
            ", ".join(list(set([r.get("state", "UNKNOWN") for r in reviews]))) or "NONE", pr.get("authorAssociation", "NONE"), 
            pr.get("reactions", {}).get("totalCount", 0), main_vec, ai_data.triage_category, extract_python_modules([p for p, _ in diff_chunks])[:4000]
        )
        user_row = (author_name, auth.get("company", "None"), auth.get("followers", {}).get("totalCount", 0), "Contributor", datetime.now(timezone.utc).isoformat()) if author_name != "Ghost" else None
        
        return {
            "pr_row": pr_row, "diff_rows": diff_rows, "comment_rows": comment_rows, "user_row": user_row, "updated_at": pr["updatedAt"],
            "issues_mapping":[(pr_num, ref["number"]) for ref in pr.get("closingIssuesReferences", {}).get("nodes",[])],
            "modifies_mapping":[(pr_num, sym) for sym in ai_data.modified_symbols]
        }

async def main():
    logger.info(f"Starting PR Sync... (FORCE_RESYNC={FORCE_RESYNC})")
    last_sync = await asyncio.to_thread(get_last_sync_time, "prs_sync")
    existing_map = await asyncio.to_thread(get_existing_prs_map)

    async with get_github_session(is_graphql=True) as session:
        await sync_repo_collaborators(session)
        prs = await fetch_incremental_prs(session, last_sync)
        if not prs: 
            logger.info("✅ No new PRs found. Already up-to-date.")
            return

        tasks =[process_pr(session, pr, i, len(prs)) for i, pr in enumerate(prs, 1) if FORCE_RESYNC or existing_map.get(int(pr["number"])) != pr["updatedAt"]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    pr_r, diff_r, c_r, iss_m, mod_m, u_map, valid_ts = [], [], [], [], [], {},[]
    for res in results:
        if res and not isinstance(res, Exception):
            pr_r.append(res["pr_row"])
            diff_r.extend(res["diff_rows"])
            c_r.extend(res["comment_rows"])
            iss_m.extend(res["issues_mapping"])
            mod_m.extend(res["modifies_mapping"])
            valid_ts.append(res["updated_at"])
            if res["user_row"]: u_map[res["user_row"][0]] = res["user_row"]

    await asyncio.to_thread(spanner_bulk_write, "Users", ("github_handle", "company", "followers", "system_role", "last_updated"), list(u_map.values()), 2000)
    await asyncio.to_thread(spanner_bulk_write, "PullRequests", ("pr_number", "title", "author", "assignees", "reviewers", "state", "url", "pr_type", "created_at", "updated_at", "components", "statuses", "other_labels", "body_text", "pr_summary", "diff_text", "ci_status", "review_decisions", "author_association", "upvotes", "embedding", "triage_category", "impacted_modules"), pr_r, 500)
    await asyncio.to_thread(spanner_bulk_write, "PRDiffNodes", ("pr_number", "node_id", "file_path", "diff_text", "embedding"), diff_r, 2000)
    await asyncio.to_thread(spanner_bulk_write, "PRComments", ("pr_number", "comment_id", "author", "created_at"), c_r, 2000)
    await asyncio.to_thread(spanner_bulk_write, "IssuePRMapping", ("pr_number", "issue_number"), iss_m, 2000)
    await asyncio.to_thread(spanner_bulk_write, "PRModifiesCode", ("pr_number", "symbol_name"), mod_m, 2000)

    if valid_ts: await asyncio.to_thread(update_sync_state, "prs_sync", max(valid_ts))
    await asyncio.sleep(0.1)
    logger.info("✅ PR Sync Complete.")

if __name__ == "__main__": asyncio.run(main())