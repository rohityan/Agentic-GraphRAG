"""Documentation Ingestion Pipeline."""
import logging
import asyncio
import hashlib

from ingestion.core.settings import REPO_OWNER, REPO_NAME
from ingestion.core.clients import ai_client
from ingestion.core.database import get_last_sync_time, update_sync_state, spanner_bulk_write
from ingestion.core.github import get_github_session

logger = logging.getLogger("DocSync")

async def get_github_content(session, owner, repo, path, is_json=False):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    async with session.get(url) as resp:
        return await resp.json() if is_json and resp.status == 200 else await resp.text() if resp.status == 200 else None

# 🚀 NEW: Recursive function to find ALL markdown files, no matter how deep the folders go!
async def get_all_md_paths(session, owner, repo, path) -> list:
    paths =[]
    contents = await get_github_content(session, owner, repo, path, is_json=True)
    if not contents or not isinstance(contents, list):
        return paths
        
    for item in contents:
        if item['type'] == 'file' and item['name'].endswith('.md'):
            paths.append(item['path'])
        elif item['type'] == 'dir':
            # Recurse into subdirectories
            sub_paths = await get_all_md_paths(session, owner, repo, item['path'])
            paths.extend(sub_paths)
    return paths

def chunk_text(text: str, chunk_size=3000):
    paragraphs, chunks, current = text.split("\n\n"),[], ""
    for p in paragraphs:
        if len(current) + len(p) > chunk_size:
            if current: chunks.append(current.strip())
            current = p + "\n\n"
        else: current += p + "\n\n"
    if current: chunks.append(current.strip())
    return chunks

async def main():
    logger.info("Starting Documentation Sync...")
    docs, hasher =[], hashlib.md5()
    
    async with get_github_session() as session:
        # 1. Repo Architecture Docs (llms.txt)
        for file_name in ["llms.txt", "llms-full.txt"]:
            content = await get_github_content(session, REPO_OWNER, REPO_NAME, file_name)
            if content:
                hasher.update(content.encode('utf-8'))
                docs.extend([{"title": f"{REPO_NAME} Architecture", "source": file_name, "content": c} for c in chunk_text(content)])

        # 2. Define the External Repositories and Folders to crawl
        external_sources =[
            {"owner": "google", "repo": "adk-docs", "path": "skills", "label": "ADK Skill"},
            {"owner": "google", "repo": "agents-cli", "path": "skills", "label": "Agents CLI Skill"} 
            # 💡 Note: If the agents-cli repo uses a 'docs' folder instead of 'skills', just change the path above!
        ]

        # 3. Crawl for all Markdown file paths
        fetch_queue =[]
        for source in external_sources:
            paths = await get_all_md_paths(session, source["owner"], source["repo"], source["path"])
            for p in paths:
                fetch_queue.append({
                    "owner": source["owner"], "repo": source["repo"], 
                    "path": p, "label": source["label"]
                })

        # 4. Download the actual content of all discovered markdown files concurrently
        fetched_contents = await asyncio.gather(*[
            get_github_content(session, item["owner"], item["repo"], item["path"]) 
            for item in fetch_queue
        ])
        
        # 5. Add them to the processing pipeline
        for item, content in zip(fetch_queue, fetched_contents):
            if content:
                hasher.update(content.encode('utf-8'))
                docs.append({"title": item["label"], "source": f"{item['repo']}/{item['path']}", "content": content})

    if not docs: 
        logger.warning("No docs found.")
        return
    
    current_hash = hasher.hexdigest()
    if current_hash == await asyncio.to_thread(get_last_sync_time, "docs_sync"):
        logger.info("✅ Docs already up-to-date.")
        return 

    logger.info(f"Uploading {len(docs)} chunks...")
    
    # 🚀 THE FIX: Lowered batch size to 5 to prevent Vertex AI token limit errors!
    batch_size = 5
    for i in range(0, len(docs), batch_size):
        batch = docs[i:i+batch_size]
        
        # We also add a safe text truncation [:8000] just to guarantee no single massive markdown block crashes it
        resp = await ai_client.aio.models.embed_content(
            model='text-embedding-004', 
            contents=[d["content"][:8000] for d in batch]
        )
        
        rows = [(hashlib.md5(f"{d['source']}_{j}".encode()).hexdigest(), d["title"], d["source"], d["content"], resp.embeddings[j].values) for j, d in enumerate(batch)]
        
        await asyncio.to_thread(spanner_bulk_write, "DocumentationNodes", ("node_id", "title", "source", "content", "embedding"), rows, 500)
        
        # 🚀 ADDED SLEEP: Protects Vertex AI quotas
        await asyncio.sleep(0.5)

    await asyncio.to_thread(update_sync_state, "docs_sync", current_hash)
    await asyncio.sleep(0.1)
    logger.info("✅ Documentation Sync Complete.")

if __name__ == "__main__": asyncio.run(main())