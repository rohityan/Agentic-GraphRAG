#!/bin/bash
# Exit immediately if any script crashes
set -e 

echo "🚀 Starting Daily GraphRAG Sync Pipeline..."

echo "📚 1. Syncing Documentation & Skills..."
python -m ingestion.sync_docs

echo "💻 2. Syncing Codebase AST..."
python -m ingestion.sync_codebase

echo "🐛 3. Syncing GitHub Issues..."
python -m ingestion.sync_issues

echo "🔄 4. Syncing Pull Requests..."
python -m ingestion.sync_prs

echo "📦 5. Syncing Releases & Diffs..."
python -m ingestion.sync_releases

echo "🤖 6. Running Deduplication Agent..."
python -m agents.dedup_agent.main

echo "✅ Daily GraphRAG Sync & Deduplication Completed Successfully!"