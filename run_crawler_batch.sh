#!/bin/bash
cd /Users/rob/repos/artifish
source .venv/bin/activate
uv run python workers/profile_crawler.py --max 10000 --delay 1.0