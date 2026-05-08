# 1. Use your exact tested Python version
FROM python:3.12.8-slim

# 2. Install System Dependencies (Git is required for codebase AST)
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

# 3. 🚀 INJECT UV: Copy the compiled uv binaries directly from the official image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 4. Set the Workspace
WORKDIR /app

# 5. Install Python Dependencies using uv
COPY requirements.txt .
# Using --system installs them globally in the container, avoiding venv overhead!
RUN uv pip install --system --no-cache -r requirements.txt

# 6. Copy Your Code
COPY . .

# 7. Set Execution Permissions
RUN chmod +x sync_all.sh

# 8. The Wake-Up Command (Triggers Ingestion + Agent)
CMD ["./sync_all.sh"]