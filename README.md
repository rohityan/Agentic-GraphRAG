# 🧠 Agentic Repo GraphRAG

**An Enterprise-Grade Repository Intelligence System.**

This project ingests an entire GitHub repository (Issues, Pull Requests, Releases, Documentation, and Python AST Codebase) into a **Google Cloud Spanner Property Graph**. It utilizes a "Two-Brain" AI architecture powered by **Vertex AI (Gemini 2.5 Pro / 2.5 Flash)** and the **Google Agent Development Kit (ADK)** to perform deterministic data extraction, background deduplication, and conversational GraphRAG.

---

## 🏗 Architecture Overview

This system is divided into three primary layers:

### 1. Deterministic Data Ingestion (`/ingestion`)
* **Highly Concurrent:** Uses `uv`, `aiohttp`, and `asyncio` to perform high-speed incremental delta-syncs from GitHub (GraphQL/REST).
* **AST Parsing:** Parses raw Python code into an Abstract Syntax Tree (AST) via `astroid`, mapping functions and classes into `CodeNodes` and `DependsOn` edges.
* **Structured Extraction:** Passes raw data through Gemini 2.5 Flash with strict `Pydantic` schemas to categorize triage states, extract technical summaries, and map impacted modules.

### 2. Background Deduplication Agent (`/agents/dedup_agent`)
* **Duplicate Detection:** Automatically checks for duplicates by evaluating semantically similar Issues and Pull Requests against each other.
* **Vector Math & Logic:** Uses Cosine Distance searches to find mathematically similar tickets, then uses LLM reasoning to determine true root causes.
* **Database Caching:** Saves this deduplication analysis natively into Spanner, saving expensive compute time for the interactive foreground agent.

### 3. Interactive GraphRAG Agent (`/agents/graphrag_agent`)
* **The Conversational Bridge:** This is the primary agent with which interaction happens, connecting the user directly to the Spanner database to answer complex questions about the repository.
* **Native Graph Traversal:** Dynamically writes Spanner GQL (`GRAPH_TABLE`) and SQL to traverse relational edges based on user prompts.
* **Safe Code Execution:** Features an isolated `uv` sandbox tool (`verify_python_snippet`) to write and run Minimal Reproducible Examples (MREs).
* **MCP Fallbacks:** Integrates the official GitHub Model Context Protocol (MCP) server as a fallback to read live tickets.

---

## 📂 Project Structure

```text
Agentic-GraphRAG/
├── schema/
│   └── spanner_graph.ddl       # Spanner Relational & Property Graph Schema
├── ingestion/                  # SYSTEM 1: Deterministic Sync Pipelines
│   ├── core/                   # Shared settings, clients, and DB utilities
│   ├── models.py               # Pydantic Schemas for AI Extraction
│   ├── sync_codebase.py        # AST parsing and embedding
│   ├── sync_docs.py            # Architecture and Skills chunking
│   ├── sync_issues.py          # GitHub GraphQL Issue parsing
│   ├── sync_prs.py             # Pull Request Git Diff parsing
│   └── sync_releases.py        # Release notes and Ghost Feature tracking
├── agents/                     # SYSTEM 2 & 3: Agentic Reasoning
│   ├── dedup_agent/            # Agent to check for semantically similar duplicates
│   └── graphrag_agent/         # Interactive agent connecting the user to Spanner DB
├── sync_all.sh                 # Unified pipeline execution script
├── Dockerfile                  # Optimized uv-based deployment image
└── requirements.txt            
```

---

## 🚀 Getting Started (Local Development)

This project uses[**uv**](https://github.com/astral-sh/uv) for lightning-fast Python dependency management.

### 1. Prerequisites
* Python 3.12+
* Git installed locally
*[Google Cloud SDK (`gcloud`)](https://cloud.google.com/sdk/docs/install)
* Node.js (for `npx` / MCP GitHub Server)

### 2. Setup Environment
```bash
# Clone the repository
git clone https://github.com/your-org/Agentic-GraphRAG.git
cd Agentic-GraphRAG

# Create and activate a virtual environment via uv
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
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
SPANNER_DATABASE_ID=your-spanner-db

# Execution Settings
FORCE_RESYNC=False
```

### 4. Authenticate with Google Cloud
Ensure your local terminal has access to your Vertex AI and Spanner APIs:
```bash
gcloud auth application-default login
gcloud config set project your-gcp-project-id
gcloud auth application-default set-quota-project your-gcp-project-id
```

### 5. Initialize the Database
Run the schema DDL in Spanner Studio or via the CLI to create the tables, vector indexes, and Property Graph:
```bash
gcloud spanner databases ddl update your-spanner-db \
    --instance=your-spanner-instance \
    --file=schema/spanner_graph.ddl
```

---

## 💻 Running the System

### Data Ingestion
Run individual sync modules incrementally:
```bash
python -m ingestion.sync_codebase
python -m ingestion.sync_issues
```
*Note: To force a complete historical backfill (ignoring database timestamps), prepend `FORCE_RESYNC=1` to the command.*

### Background Deduplication
```bash
python -m agents.dedup_agent.main
```

### Conversational GraphRAG Agent (Local Testing)
Because the GraphRAG Agent is a native Google ADK application, you can test it locally using the ADK CLI tools. First, navigate into the `agents` folder:
```bash
cd agents
```

**To test in your terminal:**
```bash
adk run graphrag_agent
```

**To test using the local Web UI:**
```bash
adk web graphrag_agent
```
*(Alternatively, you can just run `adk web` and interactively select `graphrag_agent` from the list. If port 8000 is blocked, you can append `--port 8080`).*

**Example Prompts to test:**
* *"Verify if the changes proposed in PR #5559 are already implemented in the live codebase."*
* *"Write a Spanner GQL query to traverse the `DependsOn` edge and tell me what internal functions the `InMemoryRunner` class relies on."*

---

## 🐳 Cloud Deployment

This architecture utilizes two distinct deployment strategies on Google Cloud:

### 1. Ingestion & Deduplication Pipeline (Cloud Run Job)
The ingestion pipeline and dedup worker are packaged in an optimized Docker container and run as a scheduled batch job. We utilize Google Secret Manager to securely inject the GitHub API token.

```bash
gcloud run jobs deploy your-schedule-name \
    --source . \
    --region your-region \
    --memory 2Gi \
    --task-timeout 2h \
    --service-account graphrag-your-service-account-name \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=your-project,GOOGLE_CLOUD_LOCATION=your-location,SPANNER_INSTANCE_ID=your-spanner-instance,SPANNER_DATABASE_ID=your-spanner-db,GITHUB_REPO_OWNER=repo-owner,GITHUB_REPO_NAME=repo-name" \
    --set-secrets="GITHUB_TOKEN=your-secret-name:latest"
```
*(Once deployed, you can trigger this via Google Cloud Scheduler to run automatically).*

### 2. Interactive GraphRAG Agent (Cloud Run Service)
The conversational agent is deployed as a continuously running web service. The command below deploys the agent with a built-in Web UI, custom CORS settings, and fine-tuned scaling limits.

```bash
adk deploy cloud_run \
    --adk_version="1.32.0" \
    --service_name="your-service-name" \
    --project=your-project \
    --region=your-region \
    --with_ui \
    --allow_origins="*" \
    agents.graphrag_agent.main:app \
    -- \
    --service-account=your-service-account \
    --set-env-vars="SPANNER_INSTANCE_ID=your-spanner-instance,SPANNER_DATABASE_ID=your-spanner-db" \
    --timeout=600s \
    --memory=2Gi \
    --cpu=2 \
    --max-instances=5
```

