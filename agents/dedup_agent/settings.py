import os
import logging
from dotenv import load_dotenv

load_dotenv(override=True)

# 🚀 OPTIMIZATION 1: Disable OpenTelemetry & Spanner Metrics
os.environ["GOOGLE_CLOUD_SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
os.environ["SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ["OTEL_METRICS_EXPORTER"] = "none"
os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "1"

# 🚀 OPTIMIZATION 2: Mute the harmless 'Unclosed client session' warnings
logging.getLogger("asyncio").setLevel(logging.CRITICAL)

# GCP & Spanner Config
GCP_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GCP_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
SPANNER_INSTANCE = os.getenv("SPANNER_INSTANCE_ID")
SPANNER_DATABASE = os.getenv("SPANNER_DATABASE_ID")

# Agent Orchestration Settings
CONCURRENCY_LIMIT = 5
SLEEP_BETWEEN_CHUNKS = 2

# Set to True to re-evaluate ALL historical records instead of just new ones
FORCE_RESYNC = os.getenv("FORCE_RESYNC", "False").lower() in ("1", "true", "yes", "y")

APP_NAME = "adk_dedup_agent"
USER_ID = "cron_worker"