"""Centralized application settings and environment variables."""
import os
import sys
import logging
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

REQUIRED_ENV_VARS =[
    "GITHUB_TOKEN", "GITHUB_REPO_OWNER", "GITHUB_REPO_NAME", 
    "GOOGLE_CLOUD_PROJECT", "SPANNER_INSTANCE_ID", "SPANNER_DATABASE_ID"
]

if missing :=[var for var in REQUIRED_ENV_VARS if not os.getenv(var)]:
    logging.error(f"Missing required environment variables: {', '.join(missing)}")
    sys.exit(1)

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO_OWNER = os.getenv("GITHUB_REPO_OWNER")
REPO_NAME = os.getenv("GITHUB_REPO_NAME")
GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GCP_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
SPANNER_INSTANCE = os.getenv("SPANNER_INSTANCE_ID")
SPANNER_DATABASE = os.getenv("SPANNER_DATABASE_ID")

# Allows triggering a complete historical backfill via terminal: FORCE_RESYNC=1 python ...
FORCE_RESYNC = os.getenv("FORCE_RESYNC", "False").lower() in ("1", "true", "yes", "y")
CONCURRENCY_LIMIT = 5

ADK_COMPONENTS = {
    "a2a", "agent config", "agent engine", "bq", "core", "eval", 
    "live", "mcp", "models", "services", "tools", "tracing", "web"
}