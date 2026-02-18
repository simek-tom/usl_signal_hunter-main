import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
LEADSPICKER_API_KEY = os.getenv("LEADSPICKER_API_KEY")
LEADSPICKER_CSRFTOKEN = os.getenv("LEADSPICKER_CSRFTOKEN")
NEWS_API_KEY = os.getenv("NEWS_API_KEY")

BASE_DIR   = Path(__file__).resolve().parent
DATA_DIR   = BASE_DIR / "data"

MASTER_DIR  = DATA_DIR / "master"
# legacy placeholders (not auto-created)
BACKUP_DIR  = DATA_DIR / "backups"
IMPORT_DIR  = DATA_DIR / "imports"
EXPORT_DIR  = DATA_DIR / "exports"
STAGING_DIR = DATA_DIR / "staging"

# Leadspicker folders (split into pipelines)
LP_ROOT_DIR = DATA_DIR / "leadspicker"

# Default pipeline: expansion_general_post (this backs existing LP UI/routes)
LP_GENERAL_ROOT = LP_ROOT_DIR / "expansion_general_post"
LP_FETCH_DIR = LP_GENERAL_ROOT / "fetches"
LP_NORMALIZED_DIR = LP_GENERAL_ROOT / "normalized_fetches"
LP_DROPPED_DIR = LP_GENERAL_ROOT / "dropped_duplicates"
LP_ANALYZED_DIR = LP_GENERAL_ROOT / "analyzed"
LP_ANALYSES_DIR = LP_ANALYZED_DIR  # back-compat alias for analyzed
LP_DRAFTED_DIR = LP_GENERAL_ROOT / "drafted_messages"
LP_LABELING_MEMORY_FILE = LP_GENERAL_ROOT / "labeling_memory.csv"
LP_STAGE_DIR = LP_NORMALIZED_DIR  # back-compat alias

# Secondary pipeline: expansion_czechia_post (not yet wired into routes)
LP_CZECHIA_ROOT = LP_ROOT_DIR / "expansion_czechia_post"
LP_CZECHIA_FETCH_DIR = LP_CZECHIA_ROOT / "fetches"
LP_CZECHIA_NORMALIZED_DIR = LP_CZECHIA_ROOT / "normalized_fetches"
LP_CZECHIA_DROPPED_DIR = LP_CZECHIA_ROOT / "dropped_duplicates"
LP_CZECHIA_ANALYZED_DIR = LP_CZECHIA_ROOT / "analyzed"
LP_CZECHIA_DRAFTED_DIR = LP_CZECHIA_ROOT / "drafted_messages"
LP_CZECHIA_LABELING_MEMORY_FILE = LP_CZECHIA_ROOT / "labeling_memory.csv"

# Airtable folders
AT_ROOT_DIR = DATA_DIR / "airtable"
AT_FETCH_DIR = AT_ROOT_DIR / "fetches"
AT_ANALYZED_DIR = AT_ROOT_DIR / "analyzed"

# News folders
NEWS_ROOT_DIR = DATA_DIR / "news"
NEWS_FETCH_DIR = NEWS_ROOT_DIR / "fetches"
NEWS_NORMALIZED_DIR = NEWS_ROOT_DIR / "normalized"
NEWS_ANALYZED_DIR = NEWS_ROOT_DIR / "analyzed"
NEWS_DRAFTED_DIR = NEWS_ROOT_DIR / "drafted_messages"
NEWS_LABELING_MEMORY_FILE = NEWS_ROOT_DIR / "labeling_memory.csv"
NEWS_STAGE_DIR = NEWS_NORMALIZED_DIR

# Crunchbase aliases (Airtable branch uses Crunchbase data)
CB_FETCH_DIR = AT_FETCH_DIR          # back-compat alias for fetches
CB_STAGE_DIR = AT_FETCH_DIR          # back-compat alias
CB_ANALYSES_DIR = AT_ANALYZED_DIR    # back-compat alias

# Back-compat alias
ANALYSES_DIR = LP_ANALYZED_DIR

# Master file (authoritative)
MASTER_FILE = MASTER_DIR / "master_log.csv"  # UTF-8 + ; delimiter

# Crunchbase/Airtable labeling memory (if needed)
CB_LABELING_MEMORY_FILE = AT_ROOT_DIR / "labeling_memory.csv"

# ensure dirs exist (now includes LP pipelines)
for p in [
    DATA_DIR,
    MASTER_DIR,
    # Leadspicker default (general)
    LP_ROOT_DIR,
    LP_GENERAL_ROOT,
    LP_FETCH_DIR,
    LP_NORMALIZED_DIR,
    LP_DROPPED_DIR,
    LP_ANALYZED_DIR,
    LP_DRAFTED_DIR,
    # Leadspicker czechia pipeline
    LP_CZECHIA_ROOT,
    LP_CZECHIA_FETCH_DIR,
    LP_CZECHIA_NORMALIZED_DIR,
    LP_CZECHIA_DROPPED_DIR,
    LP_CZECHIA_ANALYZED_DIR,
    LP_CZECHIA_DRAFTED_DIR,
    # Airtable / CB
    AT_ROOT_DIR,
    AT_FETCH_DIR,
    AT_ANALYZED_DIR,
    # News
    NEWS_ROOT_DIR,
    NEWS_FETCH_DIR,
    NEWS_NORMALIZED_DIR,
    NEWS_ANALYZED_DIR,
    NEWS_DRAFTED_DIR,
]:
    p.mkdir(parents=True, exist_ok=True)
