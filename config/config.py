"""
AniWorld.to HTTPX Scraper Configuration
Load credentials from .env file, set paths, and scraping options.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# ==================== CREDENTIALS ====================
EMAIL = os.getenv("ANIWORLD_EMAIL", "")
PASSWORD = os.getenv("ANIWORLD_PASSWORD", "")

# ==================== DIRECTORIES ====================
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")

Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(LOGS_DIR).mkdir(parents=True, exist_ok=True)

# Series index file
SERIES_INDEX_FILE = os.path.join(DATA_DIR, "series_index.json")

# ==================== SCRAPING SETTINGS ====================
NUM_WORKERS = int(os.getenv("ANIWORLD_MAX_WORKERS", "10"))

# ==================== LOGGING ====================
LOG_FILE = os.path.join(LOGS_DIR, "aniworld_backup.log")

# ==================== TIMEOUTS ====================
HTTP_REQUEST_TIMEOUT = 20.0

print(f"✓ Config loaded (DATA_DIR: {DATA_DIR})")
