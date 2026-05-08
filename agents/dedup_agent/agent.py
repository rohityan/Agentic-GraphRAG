import asyncio
from typing import List, Dict
from pydantic import BaseModel, Field

from google import genai
from google.genai import types
from google.cloud import spanner
from google.adk.agents import Agent
from google.adk.tools import FunctionTool
from google.adk.models.google_llm import Gemini 

from .settings import GCP_PROJECT, GCP_LOCATION
from .utils import database, increment_api_call_count

ai_client = genai.Client(vertexai=True, project=GCP_PROJECT, location=GCP_LOCATION)

class AgenticAnalysisResult(BaseModel):
    verified_duplicate_ids: List[int] = Field(description="List of ONLY issue/PR numbers sharing the exact same root cause.")
    ai_duplicate_analysis: str = Field(description="2-3 sentence technical summary explaining how these items are related.")

async def get_embedding(text: str) -> List[float]:
    increment_api_call_count()
    resp = await ai_client.aio.models.embed_content(model='text-embedding-004', contents=text[:8000])
    return resp.embeddings[0].values

async def search_issues(query_text: str, limit: int = 5) -> List[Dict]:
    vector = await get_embedding(query_text)
    sql = "SELECT issue_number, title, issue_type, state, triage_category, issue_summary FROM Issues WHERE embedding IS NOT NULL ORDER BY COSINE_DISTANCE(embedding, @v) ASC LIMIT @l"
    def _search():
        increment_api_call_count()
        with database.snapshot() as s:
            results = s.execute_sql(sql, params={"v": vector, "l": limit}, param_types={"v": spanner.param_types.Array(spanner.param_types.FLOAT32), "l": spanner.param_types.INT64})
            return [dict(zip([col.name for col in results.fields], row)) for row in results]
    return await asyncio.to_thread(_search)

async def search_prs(query_text: str, limit: int = 5) -> List[Dict]:
    vector = await get_embedding(query_text)
    sql = "SELECT pr_number, title, state, pr_type, pr_summary, SUBSTR(diff_text, 1, 2000) AS diff_snippet FROM PullRequests WHERE embedding IS NOT NULL ORDER BY COSINE_DISTANCE(embedding, @v) ASC LIMIT @l"
    def _search():
        increment_api_call_count()
        with database.snapshot() as s:
            results = s.execute_sql(sql, params={"v": vector, "l": limit}, param_types={"v": spanner.param_types.Array(spanner.param_types.FLOAT32), "l": spanner.param_types.INT64})
            return [dict(zip([col.name for col in results.fields], row)) for row in results]
    return await asyncio.to_thread(_search)

async def traverse_repository_graph(target_type: str, target_id: str) -> List[Dict]:
    """Traverses the GraphRAG_Net property graph natively."""
    target_type = target_type.upper()
    if target_type == 'PR':
        sql = "GRAPH GraphRAG_Net MATCH (p:PullRequests {pr_number: @t}) OPTIONAL MATCH (p)-[r:Resolves]->(i:Issues) OPTIONAL MATCH (rel:Releases)-[s:Ships]->(p) RETURN p.title AS pr_title, i.issue_number AS resolved_issue, i.title AS issue_title, rel.tag_name AS shipped_in_release"
        ptype, tval = spanner.param_types.INT64, int(target_id)
    elif target_type == 'ISSUE':
        sql = "GRAPH GraphRAG_Net MATCH (p:PullRequests)-[r:Resolves]->(i:Issues {issue_number: @t}) RETURN i.title AS issue_title, p.pr_number AS resolving_pr, p.title AS pr_title, p.state AS pr_state"
        ptype, tval = spanner.param_types.INT64, int(target_id)
    elif target_type == 'RELEASE':
        sql = "GRAPH GraphRAG_Net MATCH (rel:Releases {tag_name: @t})-[s:Ships]->(p:PullRequests) RETURN rel.name AS release_name, p.pr_number AS included_pr, p.title AS pr_title"
        ptype, tval = spanner.param_types.STRING, str(target_id)
    else: return[{"error": "Invalid target_type. Use PR, Issue, or Release."}]

    def _search():
        increment_api_call_count()
        with database.snapshot() as s:
            results = s.execute_sql(sql, params={"t": tval}, param_types={"t": ptype})
            return[dict(zip([col.name for col in results.fields], row)) for row in results]
    return await asyncio.to_thread(_search)

dedup_instruction = """
You are an autonomous Deduplication Agent for a GitHub repository.
When given a Target Issue or PR, you must:
1. Use your `search_issues` and `search_prs` tools to find related tickets via semantic similarity.
2. Use your `traverse_repository_graph` tool to find definitive hard links in the Property Graph.
3. Read the search results carefully to determine duplicates or shared logic.

IMPORTANT: You MUST output your final answer as a raw JSON object containing EXACTLY two keys: 
- "verified_duplicate_ids" (a list of integers of the duplicate PRs/Issues, or[] if none)
- "ai_duplicate_analysis" (a 2-3 sentence string summarizing why they are related)

Your response must be ONLY the JSON object. Do not include introductory text!
"""

dedup_agent = Agent(
    name="deduplication_agent",
    model=Gemini(model="gemini-2.5-flash"), 
    instruction=dedup_instruction,
    tools=[FunctionTool(search_issues), FunctionTool(search_prs), FunctionTool(traverse_repository_graph)],
)