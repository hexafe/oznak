#!/bin/bash

# Perform bisync operation for project
/usr/bin/rclone bisync \
  /home/hexaf/Projects/Python/oznak/ \
  gdrive_remote:_KODZENIE/PYTHON/oznak/ \
  --resync \
  --verbose \
  --compare size,modtime,checksum \
  --conflict-resolve newer \
  --exclude '.venv/**' \
  --exclude 'node_modules/**' \
  --exclude '.git/**' \
  --exclude '__pycache__/**' \
  --exclude '.pytest_cache/**' \
  --exclude '.DS_Store' \
  --exclude 'Thumbs.db' \
  --exclude '*.tmp' \
  --exclude '*.log' \
  --exclude '.idea/**' \
  --exclude '.vscode/**'
