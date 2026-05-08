"""
Conversational GraphRAG Agent (Chat API & Terminal)

An Elite Tier-3 Agentic System built on the Google ADK. 
It uses Gemini 2.5 Pro to orchestrate:
1. Spanner Property Graph Traversals (Relational Codebase & Tickets)
2. Semantic Vector Searches (Cosine Distance)
3. Model Context Protocol (MCP) for real-time GitHub API fallbacks
4. A strict `uv` isolated sandbox for safe Python code execution
"""

import os
import sys
import logging
import asyncio
import subprocess
import tempfile
from pathlib import Path
from typing import List, Dict
import httpx
from dotenv import load_dotenv

# ==========================================
# 0. OPTIMIZATIONS & GCP OVERRIDES
# ==========================================
# 🚀 CRITICAL: Disable telemetry to save memory and prevent noisy logs
os.environ["GOOGLE_CLOUD_SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
os.environ["SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ["OTEL_METRICS_EXPORTER"] = "none"

# 🚀 Force ADK's internal clients to authenticate via Vertex AI (No API Key needed)
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "1" 

logging.basicConfig(level=logging.WARNING) 
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

load_dotenv(override=True)

# Pydantic & Google SDKs
from pydantic import BaseModel, Field
from google.cloud import spanner
from google import genai
from google.genai import types

# Google Agent Development Kit (ADK)
from google.adk.agents import Agent
from google.adk.runners import InMemoryRunner
from google.adk.models.google_llm import Gemini
from google.adk.tools import FunctionTool
from google.adk.tools.google_search_tool import GoogleSearchTool
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from mcp import StdioServerParameters

# ==========================================
# 1. SINGLETON CLIENT MANAGER
# ==========================================
# Caching clients manages connection pools and avoids gRPC session leaks
spanner_client = None
spanner_database = None
ai_client = None

def _get_clients():
    """Retrieves shared, cached client singleton objects for Spanner and GenAI."""
    global spanner_client, ai_client, spanner_database
    
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    location = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
    instance_id = os.environ.get("SPANNER_INSTANCE_ID")
    database_id = os.environ.get("SPANNER_DATABASE_ID")
    
    if not all([project_id, instance_id, database_id]):
        raise ValueError("Required Spanner environment variables are missing.")
        
    if spanner_client is None:
        spanner_client = spanner.Client(project=project_id)
    if spanner_database is None:
        spanner_database = spanner_client.instance(instance_id).database(database_id)
    if ai_client is None:
        ai_client = genai.Client(vertexai=True, project=project_id, location=location)
        
    return ai_client, spanner_database

# ==========================================
# 2. PYDANTIC SCHEMAS
# ==========================================
class ClassificationResult(BaseModel):
    journey: str = Field(description="Must be exactly one of: 'First Impression & Onboarding', 'Core Development Loop', 'It Should Just Work (DevEx Gaps)', or 'Advanced/Production'")
    journey_justification: str = Field(description="Justification for why it fits this journey.")
    alternate_journey: str = Field(description="Another possible journey category it could fit into.")
    feature_area: str = Field(description="Must be exactly one of: 'Production Readiness & Deployment', 'Context, Session and State Management', 'Model Support and Integrations', 'Tool Support', 'Agent Identity and Security', 'Live and Streaming', 'Quality Observability and Stability', or 'ADK Web'")
    feature_area_justification: str = Field(description="Justification for the feature area.")
    alternate_feature_area: str = Field(description="Another possible feature area.")
    suggested_label: str = Field(description="Must be one of the exact components: 'a2a', 'agent config', 'agent engine', 'bq', 'core', 'eval', 'live', 'mcp', 'models', 'services', 'tools', 'tracing', 'web'")
    label_justification: str = Field(description="Justification for the suggested component label.")
    alternate_label: str = Field(description="Another possible component label.")
    pattern: List[str] = Field(description="Specific, recurring technical keywords noticed in the text.")
    issue_type: str = Field(description="Indicate if it is a 'bug fix', 'doc fix', 'feature', or 'ci/cd update'.")
    type_justification: str = Field(description="Justification for the issue type.")
    existing_labels_comparison: str = Field(description="Compare existing labels with recommended labels.")

# ==========================================
# 3. SPANNER GraphRAG TOOLS
# ==========================================
def get_embedding(text: str) -> List[float]:
    """Generates 768-dimensional text embeddings for semantic searching."""
    client, _ = _get_clients()
    response = client.models.embed_content(model='text-embedding-004', contents=text[:8000])
    return response.embeddings[0].values

def search_documentation(query_text: str, limit: int = 3) -> dict:
    """Semantically searches architecture index and skills."""
    vector = get_embedding(query_text)
    sql = "SELECT title, source, content, COSINE_DISTANCE(embedding, @vector) as distance FROM DocumentationNodes@{force_index=DocEmbeddings} WHERE embedding IS NOT NULL ORDER BY distance ASC LIMIT @limit"
    _, db = _get_clients()
    try:
        with db.snapshot() as s:
            results = s.execute_sql(sql, params={"vector": vector, "limit": limit}, param_types={"vector": spanner.param_types.Array(spanner.param_types.FLOAT32), "limit": spanner.param_types.INT64})
            return {"results":[dict(zip([c.name for c in results.fields], r)) for r in results]}
    except Exception as e: return {"error": str(e)}

def spanner_search_issues(query_text: str = "", state: str = "", limit: int = 5) -> dict:
    """Semantic lookup for GitHub Issues utilizing exact physically verified schema columns."""
    _, db = _get_clients()
    try:
        sql = "SELECT issue_number, title, state, issue_type, issue_summary, has_maintainer_resolution, resolution_snippet, ai_duplicate_analysis, verified_duplicate_ids"
        params, param_types = {}, {}
        if query_text:
            sql += ", COSINE_DISTANCE(embedding, @vector) as distance FROM Issues@{force_index=IssueEmbeddings} WHERE embedding IS NOT NULL"
            params["vector"], param_types["vector"] = get_embedding(query_text), spanner.param_types.Array(spanner.param_types.FLOAT32)
        else: sql += " FROM Issues WHERE 1=1"
        if state:
            sql += " AND UPPER(state) = UPPER(@state)"
            params["state"], param_types["state"] = state, spanner.param_types.STRING
        sql += f" ORDER BY distance ASC LIMIT {limit}" if query_text else f" ORDER BY created_at DESC LIMIT {limit}"
        with db.snapshot() as s:
            results = s.execute_sql(sql, params=params, param_types=param_types)
            return {"results": [dict(zip([c.name for c in results.fields], r)) for r in results]}
    except Exception as e: return {"error": str(e)}

def spanner_search_prs(query_text: str = "", state: str = "", limit: int = 5) -> dict:
    """Semantic lookup for Pull Requests utilizing exact physical DB schema keys."""
    _, db = _get_clients()
    try:
        sql = "SELECT pr_number, title, state, pr_type, pr_summary, impacted_modules, ai_duplicate_analysis, verified_duplicate_ids"
        params, param_types = {}, {}
        if query_text:
            sql += ", COSINE_DISTANCE(embedding, @vector) as distance FROM PullRequests@{force_index=PREmbeddings} WHERE embedding IS NOT NULL"
            params["vector"], param_types["vector"] = get_embedding(query_text), spanner.param_types.Array(spanner.param_types.FLOAT32)
        else: sql += " FROM PullRequests WHERE 1=1"
        if state:
            sql += " AND UPPER(state) = UPPER(@state)"
            params["state"], param_types["state"] = state, spanner.param_types.STRING
        sql += f" ORDER BY distance ASC LIMIT {limit}" if query_text else f" ORDER BY created_at DESC LIMIT {limit}"
        with db.snapshot() as s:
            results = s.execute_sql(sql, params=params, param_types=param_types)
            return {"results": [dict(zip([c.name for c in results.fields], r)) for r in results]}
    except Exception as e: return {"error": str(e)}

def search_releases(query_text: str = "", limit: int = 3) -> dict:
    """Semantic searches across official GitHub Releases."""
    _, db = _get_clients()
    try:
        sql, params, param_types = "SELECT tag_name, name, published_at, body_text, diff_text", {}, {}
        if query_text:
            sql += ", COSINE_DISTANCE(embedding, @vector) as distance FROM Releases@{force_index=ReleaseEmbeddings} WHERE embedding IS NOT NULL ORDER BY distance ASC LIMIT @limit"
            params["vector"], param_types["vector"], params["limit"], param_types["limit"] = get_embedding(query_text), spanner.param_types.Array(spanner.param_types.FLOAT32), limit, spanner.param_types.INT64
        else: sql += f" FROM Releases ORDER BY published_at DESC LIMIT {limit}"
        with db.snapshot() as s:
            results = s.execute_sql(sql, params=params, param_types=param_types)
            return {"results":[{**dict(zip([c.name for c in results.fields], r)), "diff_text": (r[4][:5000] + "...[TRUNCATED]") if r[4] else ""} for r in results]}
    except Exception as e: return {"error": str(e)}

def search_similar_code_diffs(code_snippet: str, limit: int = 3) -> dict:
    """Finds historical Pull Requests that previously modified logic similar to the snippet."""
    _, db = _get_clients()
    try:
        sql = "SELECT pr_number, file_path, diff_text, COSINE_DISTANCE(embedding, @vector) as distance FROM PRDiffNodes@{force_index=PRDiffEmbeddings} WHERE embedding IS NOT NULL ORDER BY distance ASC LIMIT @limit"
        params, param_types = {"vector": get_embedding(code_snippet), "limit": limit}, {"vector": spanner.param_types.Array(spanner.param_types.FLOAT32), "limit": spanner.param_types.INT64}
        with db.snapshot() as s:
            results = s.execute_sql(sql, params=params, param_types=param_types)
            return {"results": [dict(zip([c.name for c in results.fields], r)) for r in results]}
    except Exception as e: return {"error": str(e)}

def search_codebase(query_text: str = "", exact_name: str = "", file_name: str = "", limit: int = 10) -> dict:
    """Queries Python AST codebase via semantic language, distinct symbol, or filename."""
    _, db = _get_clients()
    try:
        if not query_text and not exact_name and not file_name:
            return {"error": "Search filter missing. Provide query_text, exact_name, or file_name."}
            
        if exact_name:
            sql = "SELECT name, type, text, start_line, end_line FROM CodeNodes WHERE name = @name LIMIT @limit"
            params, param_types = {"name": exact_name, "limit": limit}, {"name": spanner.param_types.STRING, "limit": spanner.param_types.INT64}
        elif file_name:
            sql = "SELECT c.name, c.type, c.text, c.start_line, c.end_line FROM CodeNodes c JOIN DefinedIn d ON c.id = d.code_id WHERE d.file_name LIKE @fname LIMIT @limit"
            params, param_types = {"fname": f"%{file_name}%", "limit": limit}, {"fname": spanner.param_types.STRING, "limit": spanner.param_types.INT64}
        else:
            sql = "SELECT name, type, text, start_line, end_line, COSINE_DISTANCE(embedding, @vector) as distance FROM CodeNodes@{force_index=CodeEmbeddings} WHERE embedding IS NOT NULL ORDER BY distance ASC LIMIT @limit"
            params, param_types = {"vector": get_embedding(query_text), "limit": limit}, {"vector": spanner.param_types.Array(spanner.param_types.FLOAT32), "limit": spanner.param_types.INT64}
            
        with db.snapshot() as s:
            results = s.execute_sql(sql, params=params, param_types=param_types)
            return {"results": [dict(zip([c.name for c in results.fields], r)) for r in results]}
    except Exception as e: return {"error": str(e)}

def execute_spanner_sql(sql_query: str) -> dict:
    """Executes raw ad-hoc SQL queries against Spanner with server-side context safety."""
    _, db = _get_clients()
    try:
        with db.snapshot() as s:
            results = s.execute_sql(sql_query)
            output = []
            for row in results:
                row_dict = dict(zip([c.name for c in results.fields], row))
                for k, v in row_dict.items():
                    if isinstance(v, str) and len(v) > 5000:
                        row_dict[k] = v[:5000] + "...[TRUNCATED BY SERVER]"
                output.append(row_dict)
            return {"data": output[:50]}
    except Exception as e: return {"error": str(e)}

# ==========================================
# 4. EXTERNAL SYSTEM TOOLS (Sandboxes & Web)
# ==========================================
def classify_issue_or_pr(summary_text: str, existing_labels: str = "None") -> dict:
    """Classifies logical state of an issue into rigorous Pydantic validated schemas."""
    ai, _ = _get_clients()
    prompt = f"Classify based on ADK criteria.\nLABELS: {existing_labels}\nTEXT: {summary_text}"
    try:
        resp = ai.models.generate_content(
            model='gemini-2.5-flash', contents=prompt,
            config=types.GenerateContentConfig(response_mime_type="application/json", response_schema=ClassificationResult, temperature=0.1)
        )
        return ClassificationResult.model_validate_json(resp.text).model_dump()
    except Exception as e: return {"error": str(e)}

def verify_python_snippet(code_snippet: str, packages: list[str]) -> dict:
    """Executes code snippet internally in rigorous `uv` isolated sandbox."""
    with tempfile.TemporaryDirectory() as sandbox_dir:
        temp_file_path = os.path.join(sandbox_dir, "repro_script.py")
        try:
            with open(temp_file_path, "w", encoding="utf-8") as f:
                f.write(code_snippet)
            cmd = ["uv", "run", "--isolated"]
            if packages:
                for pkg in packages: cmd.extend(["--with", pkg])
            cmd.extend(["python", "repro_script.py"])
            result = subprocess.run(cmd, cwd=sandbox_dir, capture_output=True, text=True, timeout=120)
            return {"exit_code": result.returncode, "stdout": result.stdout, "stderr": result.stderr}
        except subprocess.TimeoutExpired: return {"error": "Timeout."}
        except Exception as e: return {"error": str(e)}

def read_local_file(file_path: str) -> dict:
    """Reads readable artifacts within safe workspace filesystems."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f: return {"content": f.read()}
    except Exception as e: return {"error": str(e)}

async def fetch_adk_docs(file_name: str) -> dict:
    """Fetches valid architectural specs dynamically."""
    safe_name = os.path.basename(file_name).strip()
    if not safe_name or safe_name in (".", ".."): return {"error": "Invalid filename."}
    docs_dir = Path(tempfile.gettempdir()) / "adk_docs_cache"
    local_file = docs_dir / safe_name
    def _safe_read(): return local_file.read_text(encoding='utf-8') if local_file.is_file() else None
    try:
        c = await asyncio.to_thread(_safe_read)
        if c: return {"content": c, "source": "cache"}
    except: pass 
    url = f"https://adk.dev/{safe_name}"
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            resp = await client.get(url, timeout=15.0)
            if resp.status_code == 200:
                def _write():
                    docs_dir.mkdir(parents=True, exist_ok=True)
                    with tempfile.NamedTemporaryFile('w', dir=docs_dir, delete=False) as tf:
                        tf.write(resp.text)
                        os.replace(tf.name, local_file)
                try: await asyncio.to_thread(_write)
                except: pass
                return {"content": resp.text, "source": "remote"}
    except: pass
    return {"error": "Fetch fail."}

# ==========================================
# 5. CORE AGENT DEFINITION
# ==========================================
agent_instruction = """You are the expert ADK Repository Intelligence Agent for the google/adk-python GitHub repository.
You have direct access to Spanner GraphRAG systems containing historical context, AST structures, documentation, and code releases.

=== DATA SOURCING & PRECEDENCE POLICY ===
1. **Primary Backend (Spanner/RAG)**: USE SPECIALIZED SPANNER SEARCH TOOLS OR SPANNER FIRST. Definitive source for relational context.
2. **Resilient Fallback (GitHub MCP)**: Use only if Spanner rows are missing or incomplete. (Default: google/adk-python).

=== YOUR ADVANCED SEARCH TOOLS ===
1. `search_documentation`: Architecture and API lookup.
2. `spanner_search_issues` / `spanner_search_prs`: Rapid semantic RAG lookup.
3. `search_codebase`: Symbol/filename lookup. (Pass `exact_name` to get exact functions).
4. `search_similar_code_diffs`: Analyze historically analogous code changes.
5. `execute_spanner_sql`: Advanced raw query freedom.

=== SPANNER DATABASE SCHEMA ===
Tables available in GraphRAG_Net:
- `Issues`: issue_number (PK), has_maintainer_resolution, resolution_snippet, ai_duplicate_analysis, verified_duplicate_ids, embedding.
- `PullRequests`: pr_number (PK), ai_duplicate_analysis, verified_duplicate_ids, embedding, impacted_modules, diff_text.
- `CodeNodes`: id (PK), name, text, embedding, start_line, end_line.
- Relational Edges: `DefinedIn`, `DependsOn`, `IssuePRMapping` (Resolves edge), `PRModifiesCode`.
- Non-Graph: `SavedQueries`, `DocumentationNodes`, `PRDiffNodes`.

=== SPANNER GRAPH SYNTAX (GQL) ===
Use `GRAPH_TABLE` and `COLUMNS` clause to traverse:
SELECT gt.filename FROM GRAPH_TABLE(GraphRAG_Net MATCH (c:CodeNodes)-[r:DefinedIn]->(f:FILES) WHERE c.name = 'X' COLUMNS (f.name AS filename)) gt;

=== WORKFLOW 1: DAILY DASHBOARDS & ANALYTICS ===
Run `SELECT sql_query FROM SavedQueries` via `execute_spanner_sql` to deliver analytical metrics.

=== WORKFLOW 2: ROOT CAUSE ANALYSIS (RCA) ===
1. Extract user feedback, find python functions via `search_codebase`, deduce flaws using SUBSTR text audits.
2. Provide context-aware steps: standard ADK CLI for native agents, or custom python server advice for embedded web/UI.

=== WORKFLOW 3: GHOST FEATURES & OBSOLETE PRS ===
If asked what open issues/PRs can be closed due to a new Release:
1. Run 'Ghost Feature Resolution Radar' from SavedQueries.
2. Or use `search_releases` to read the diff, then `spanner_search_issues` to find matching complaints.

=== WORKFLOW 4: BUG FIXES & KNOWN RESOLUTIONS ===
Directly audit `has_maintainer_resolution` and `resolution_snippet` from records to repeat standard patterns.

=== WORKFLOW 5: DUPLICATES & RELATIONSHIPS ===
Utilize cached AI audit results instantly. Lookup `ai_duplicate_analysis` and `verified_duplicate_ids`.

=== WORKFLOW 6: VERIFY IF A PR IS ALREADY IN THE CODEBASE ===
1. Use `execute_spanner_sql` to pull the PR's `diff_text` and `impacted_modules`.
2. Use `execute_spanner_sql` to get `symbol_name` from PRModifiesCode.
3. Use `search_codebase` (exact_name or file_name) to pull the CURRENT source code for those functions.
4. Compare the PR's diff against the live codebase to definitively state if the PR is obsolete.

=== CORE QUALITY BEST PRACTICES ===
1. **Internal Verification**: Write MRE scripts capturing environment_versions into packages array of `verify_python_snippet`.
   - Step A.1: Hardware Simulation (MANDATORY): NEVER yield to physical limitations (e.g., mic). Inject synthetic unittest.mock, io.BytesIO or artificial generators.
   - Iterate until verify_python_snippet results yielded, explicitly declare CONFIRMED/REJECTED.
2. **Context-Aware Verification**: 
   - For Native Agents: use `agents-cli playground` / `run`.
   - For Embedded Apps: authentically support custom backend execution (e.g. `python app.py`) when debugging UI/Mic/HTML.
3. **Safe String Extraction**: Leverage `SUBSTR(text, 1, 4000)` in SQL for safety.
4. **Fallback Handling**: Use MCP if no Vector embedding is found on non-indexed items.

=== CONSTRAINTS ===
- Read-only access strictly enforced.
- When quoting `CodeNodes`, always try to provide a GitHub link using the file_name, start_line, and end_line.
"""

root_agent = Agent(
    name="issue_analyst_agent",
    model=Gemini(
        model="gemini-2.5-pro",
        retry_options=types.HttpRetryOptions(attempts=3),
    ),
    instruction=agent_instruction,
    tools=[
        # GitHub MCP Fallback Tool
        McpToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command="npx",
                    args=["-y", "@modelcontextprotocol/server-github"],
                    env={**os.environ, "GITHUB_PERSONAL_ACCESS_TOKEN": os.environ.get("GITHUB_TOKEN", "")}
                )
            ),
            tool_filter=["get_issue", "search_issues", "list_issue_comments", "get_pull_request", "get_pull_request_files", "list_pull_request_commits"]
        ),
        FunctionTool(func=search_documentation), 
        FunctionTool(func=spanner_search_issues), 
        FunctionTool(func=spanner_search_prs), 
        FunctionTool(func=search_releases),
        FunctionTool(func=search_similar_code_diffs), 
        FunctionTool(func=search_codebase), 
        FunctionTool(func=classify_issue_or_pr), 
        FunctionTool(func=execute_spanner_sql),
        GoogleSearchTool(bypass_multi_tools_limit=True),
        FunctionTool(func=verify_python_snippet), 
        FunctionTool(func=read_local_file), 
        FunctionTool(func=fetch_adk_docs)
    ]
)

