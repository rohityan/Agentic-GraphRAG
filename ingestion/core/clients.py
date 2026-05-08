"""Singleton initializations for GCP Cloud Services."""
import os
import asyncio

# 🚀 OPTIMIZATION: Disable Spanner built-in metrics and OpenTelemetry 
# MUST be set before importing google.cloud or google.genai
os.environ["GOOGLE_CLOUD_SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
os.environ["SPANNER_ENABLE_BUILTIN_METRICS"] = "false"
os.environ["OTEL_SDK_DISABLED"] = "true"
os.environ["OTEL_METRICS_EXPORTER"] = "none"

from google import genai
from google.cloud import spanner
from .settings import GCP_PROJECT, GCP_LOCATION, SPANNER_INSTANCE, SPANNER_DATABASE, CONCURRENCY_LIMIT

# AI Client & Semaphore to prevent Quota Limits
ai_client = genai.Client(vertexai=True, project=GCP_PROJECT, location=GCP_LOCATION)
ai_semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

# Spanner Database Singleton
spanner_client = spanner.Client(project=GCP_PROJECT)
database = spanner_client.instance(SPANNER_INSTANCE).database(SPANNER_DATABASE)