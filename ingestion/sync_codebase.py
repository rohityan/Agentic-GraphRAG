"""Codebase Graph AST Ingestion Pipeline."""
import os
import sys
import logging
import asyncio
import hashlib
import tempfile
from pathlib import Path
import astroid
from google.cloud import spanner

from ingestion.core.settings import REPO_OWNER, REPO_NAME, GITHUB_TOKEN
from ingestion.core.clients import ai_client, database
from ingestion.core.database import get_last_sync_time, update_sync_state, spanner_bulk_write

logger = logging.getLogger("CodebaseSync")
SECURE_REPO_URL = f"https://oauth2:{GITHUB_TOKEN}@github.com/{REPO_OWNER}/{REPO_NAME}.git"

async def run_cmd(cmd, cwd=None):
    env = os.environ.copy()
    env["GIT_TERMINAL_PROMPT"] = "0"
    process = await asyncio.create_subprocess_exec(*cmd, cwd=cwd, env=env, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"Command failed: {stderr.decode().strip().replace(GITHUB_TOKEN, '***')}")
    return stdout.decode().strip()

def cleanup_spanner(files_to_delete):
    if not files_to_delete: return
    for i in range(0, len(files_to_delete), 500):
        chunk = files_to_delete[i:i+500]
        def _txn(t):
            t.execute_update(
                "DELETE FROM DependsOn WHERE code_id IN (SELECT code_id FROM DefinedIn WHERE file_name IN UNNEST(@f))", 
                {"f": chunk}, 
                {"f": spanner.param_types.Array(spanner.param_types.STRING)}
            )
            t.execute_update(
                "DELETE FROM DefinedIn WHERE file_name IN UNNEST(@f)", 
                {"f": chunk}, 
                {"f": spanner.param_types.Array(spanner.param_types.STRING)}
            )
            t.execute_update("DELETE FROM CodeNodes WHERE id NOT IN (SELECT code_id FROM DefinedIn)", {})
            t.execute_update(
                "DELETE FROM Files WHERE name IN UNNEST(@f)", 
                {"f": chunk}, 
                {"f": spanner.param_types.Array(spanner.param_types.STRING)}
            )
        database.run_in_transaction(_txn)

def extract_ast(full_path, repo_root):
    rel_path, dataset = str(full_path.relative_to(repo_root)),[]
    try:
        with open(full_path, "r", encoding="utf-8") as f: source = f.readlines()
        module = astroid.MANAGER.ast_from_file(str(full_path))
        for node in module.nodes_of_class((astroid.nodes.FunctionDef, astroid.nodes.ClassDef)):
            if not node.lineno or not node.tolineno: continue
            deps = set()
            for call in node.nodes_of_class(astroid.nodes.Call):
                try:
                    inf = next(call.func.infer(), None)
                    if inf and isinstance(inf, (astroid.nodes.FunctionDef, astroid.nodes.ClassDef)) and not inf.root().name.startswith("builtins"):
                        deps.add(inf.name)
                except Exception: pass
            
            node_type = "Class" if isinstance(node, astroid.nodes.ClassDef) else "Function/Method"
            doc = node.doc_node.value.strip() if node.doc_node else "None."
            
            dataset.append({
                "id": hashlib.md5(f"{rel_path}::{node.name}".encode()).hexdigest(),
                "file": rel_path, 
                "name": node.name, 
                "type": node_type, 
                "dependencies": list(deps),
                "start_line": node.lineno,
                "end_line": node.tolineno,
                "text": f"File: {rel_path}\nSymbol: {node.name} ({node_type})\nLines: {node.lineno}-{node.tolineno}\nDocstring: {doc}\nCode:\n{''.join(source[node.lineno-1:node.tolineno])}"
            })
            
        astroid.MANAGER.clear_cache()
    except Exception as e: 
        logger.warning(f"AST failed for {rel_path}: {e}")
    return dataset

async def main():
    logger.info("Starting Codebase Sync...")
    last_commit = await asyncio.to_thread(get_last_sync_time, "codebase_sync")
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        repo_dir = os.path.join(tmp_dir, REPO_NAME)
        await run_cmd(["git", "clone", SECURE_REPO_URL, REPO_NAME], cwd=tmp_dir)
        cur_commit = await run_cmd(["git", "rev-parse", "HEAD"], cwd=repo_dir)
        
        if last_commit == cur_commit: return logger.info("✅ Codebase already up to date.")
        
        # Inject repo dir to sys.path so astroid resolves cross-file imports properly
        sys.path.insert(0, repo_dir) 
        
        added, deleted = [], []
        if last_commit:
            try:
                diff_cmd =["git", "diff", "--name-status", "--no-renames", last_commit, cur_commit]
                for line in (await run_cmd(diff_cmd, cwd=repo_dir)).split("\n"):
                    if not line.strip().endswith(".py"): continue
                    status, path = line.split("\t", 1)
                    deleted.append(path) if status.startswith("D") else added.append(path)
            except RuntimeError: 
                last_commit = None 
            
        if not last_commit: 
            added = [f for f in (await run_cmd(["git", "ls-files"], cwd=repo_dir)).split("\n") if f.strip().endswith(".py")]

        await asyncio.to_thread(cleanup_spanner, deleted + added)
        
        all_ast =[]
        total_files = len(added)
        for idx, f in enumerate(added, 1): 
            logger.info(f"[{idx}/{total_files}] Parsing AST for {f}...")
            all_ast.extend(await asyncio.to_thread(extract_ast, Path(repo_dir) / f, Path(repo_dir)))

        # 🚀 ULTRA-SAFE LIMITS: Batch size 4, truncated to 4000 chars per item
        batch_size = 4
        for i in range(0, len(all_ast), batch_size):
            batch = all_ast[i:i+batch_size]
            
            embeds = (await ai_client.aio.models.embed_content(
                model='text-embedding-004', 
                contents=[a["text"][:4000] for a in batch]
            )).embeddings
            
            f_mut, c_mut, di_mut, do_mut, d_mut = [], [], [], [],[]
            
            for j, item in enumerate(batch):
                f_mut.append((item["file"],))
                c_mut.append((item["id"], item["name"], item["type"], item["text"], embeds[j].values, item["start_line"], item["end_line"]))
                di_mut.append((item["id"], item["file"]))
                for dep in item["dependencies"]:
                    d_mut.append((dep,))
                    do_mut.append((item["id"], dep))

            await asyncio.to_thread(spanner_bulk_write, "Files", ("name",), list(set(f_mut)))
            await asyncio.to_thread(spanner_bulk_write, "Dependencies", ("name",), list(set(d_mut)))
            await asyncio.to_thread(spanner_bulk_write, "CodeNodes", ("id", "name", "type", "text", "embedding", "start_line", "end_line"), c_mut)
            await asyncio.to_thread(spanner_bulk_write, "DefinedIn", ("code_id", "file_name"), di_mut)
            await asyncio.to_thread(spanner_bulk_write, "DependsOn", ("code_id", "dep_name"), do_mut)
            
            # Safe backoff for Vertex AI quotas
            await asyncio.sleep(0.5)

        await asyncio.to_thread(update_sync_state, "codebase_sync", cur_commit)
        sys.path.remove(repo_dir)
        logger.info("✅ Codebase Sync Complete.")

if __name__ == "__main__": asyncio.run(main())