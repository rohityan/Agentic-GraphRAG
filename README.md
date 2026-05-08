***

# 🧠 Agentic Repo GraphRAG

**An Enterprise-Grade Repository Intelligence and Analytics System.**

This project ingests an entire GitHub repository (Issues, Pull Requests, Releases, Documentation, and Python AST Codebase) into a **Google Cloud Spanner Property Graph**. It utilizes a "Two-Brain" AI architecture powered by **Vertex AI (Gemini 2.5)** to perform deterministic data extraction, followed by asynchronous agentic reasoning to find root causes, duplicate issues, and cross-ticket logic.

## 🏗 Architecture: The "Two-Brain" System

1. **System 1: Ingestion Pipeline (`/ingestion`)**
   * **Fast & Deterministic:** Periodically syncs GitHub GraphQL/REST APIs and raw Git diffs.
   * **Structured Extraction:** Uses Gemini 2.5 Flash with strict `Pydantic` schemas to categorize triage states, summarize PR diffs, and extract exact Python modules.
   * **GraphRAG Foundation:** Embeds data using `text-embedding-004` and builds definitive Spanner edges (e.g., `(PullRequest)-[Resolves]->(Issue)`, `(CodeNode)-[DependsOn]->(CodeNode)`).

2. **System 2: Agentic Reviewer (`/agents/reviewer_agent`)**
   * **Asynchronous & Exploratory:** Built on the Google ADK framework.
   * **Nightly Triage:** Automatically wakes up to review un-analyzed PRs and Issues.
   * **Graph Traversal:** Uses tool-calling (`traverse_repository_graph`, `search_codebase`) to semantically and structurally map duplicates and suggest maintainer actions.

---

## 🚀 Getting Started (Local Development)

This project uses [**uv**](https://github.com/astral-sh/uv) for lightning-fast Python dependency management.

### 1. Prerequisites
* Python 3.12+
* Git installed locally
*[Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install)
* A Google Cloud Project with Vertex AI and Cloud Spanner enabled.

### 2. Install `uv` and Setup Environment
```bash
# Install uv (if you haven't already)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone the repository
git clone https://github.com/your-org/agentic-repo-graphrag.git
cd agentic-repo-graphrag

# Create and activate a virtual environment
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies instantly
uv pip install -r requirements.txt
```

### 3. Environment Variables
Create a `.env` file in the root directory:
```env
# GitHub Auth
GITHUB_TOKEN=ghp_your_personal_access_token_here
GITHUB_REPO_OWNER=google
GITHUB_REPO_NAME=adk-python

# Google Cloud Config
GOOGLE_CLOUD_PROJECT=your-gcp-project-id
GOOGLE_CLOUD_LOCATION=us-central1
SPANNER_INSTANCE_ID=your-spanner-instance
SPANNER_DATABASE_ID=your-spanner-database

# Execution Settings (Leave False for standard incremental syncs)
FORCE_RESYNC=False
```

### 4. Authenticate with Google Cloud
Ensure your local terminal has access to your Vertex AI and Spanner APIs:
```bash
gcloud auth application-default login
gcloud config set project your-gcp-project-id
gcloud auth application-default set-quota-project your-gcp-project-id
```

---

## 💻 Running the Pipelines

You can run individual modules directly from the root of the project using Python's `-m` flag.

### Run Incremental Syncs
```bash
# Sync specific pipelines
python -m ingestion.sync_docs
python -m ingestion.sync_codebase
python -m ingestion.sync_issues
python -m ingestion.sync_prs
python -m ingestion.sync_releases

# Run the Nightly Review Agent
python -m agents.reviewer_agent.main
```

### Force a Complete Historical Backfill
By default, the sync scripts only fetch data modified since the last successful run. To override the database cursors and force a complete re-evaluation of historical data, pass the `FORCE_RESYNC=1` flag:
```bash
FORCE_RESYNC=1 python -m ingestion.sync_prs
```

### Run the Entire Unified Flow
To test the exact sequence that runs in production:
```bash
./sync_all.sh
```

---

## 🐳 Docker & Production Deployment

The project is packaged via a highly-optimized Docker container utilizing `uv --system` installs and disabling background OTEL/Spanner metrics for cost efficiency. 

To deploy as a unified **Google Cloud Run Job**:
1. Build the container.
2. Deploy the job targeting the `./sync_all.sh` entry point.
3. Schedule the execution using **Google Cloud Scheduler** (e.g., `0 */4 * * *` for every 4 hours).

---

## 📂 Project Structure

```text
agentic-repo-graphrag/
├── Dockerfile                  # Optimized uv-based deployment image
├── requirements.txt            
├── sync_all.sh                 # Unified execution script for Cloud Run
│
├── ingestion/                  # SYSTEM 1: Deterministic Sync Pipelines
│   ├── core/                   # Shared Spanner, Vertex AI, and GitHub utilities
│   ├── models.py               # Strict Pydantic LLM Output Schemas
│   ├── sync_codebase.py        # Python AST ASTroid parsing
│   ├── sync_docs.py            # Architecture and Skill MD chunking
│   ├── sync_issues.py          # GitHub GraphQL Issue parsing
│   ├── sync_prs.py             # Pull Request Git Diff parsing
│   └── sync_releases.py        # Release notes and commit diffs
│
├── agents/                     # SYSTEM 2: Agentic Reasoning
│   └── reviewer_agent/         # Nightly duplicate & root-cause analyzer
│       ├── agent.py            # GraphRAG Tools and ADK prompt definition
│       └── main.py             # Async worker loop
│
└── schema/
    └── spanner_graph.ddl       # Spanner Relational & Property Graph Schema
```

---
*Built with Google Cloud Spanner, Vertex AI Gemini 2.5, and the Google ADK.*