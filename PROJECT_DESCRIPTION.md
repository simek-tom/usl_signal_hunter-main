# USL Signal Hunter — Complete Project Description

> **Purpose**: A Flask-based internal lead-processing suite that integrates three data sources — **Leadspicker**, **Crunchbase (via Airtable)**, and **News API** — into a unified pipeline for discovering, qualifying, enriching, drafting outreach messages, and pushing contacts to external systems.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Technology Stack & Dependencies](#technology-stack--dependencies)
3. [Configuration & Environment](#configuration--environment)
4. [Data Storage Model](#data-storage-model)
5. [Master Schema](#master-schema)
6. [Pipeline 1: Leadspicker (General)](#pipeline-1-leadspicker-general)
7. [Pipeline 2: Leadspicker (Czech)](#pipeline-2-leadspicker-czech)
8. [Pipeline 3: Crunchbase / Airtable](#pipeline-3-crunchbase--airtable)
9. [Pipeline 4: News](#pipeline-4-news)
10. [Cross-Cutting Functionality](#cross-cutting-functionality)
11. [Static / Placeholder Pages](#static--placeholder-pages)
12. [Complete Route Map](#complete-route-map)
13. [Complete Function & Helper Index](#complete-function--helper-index)
14. [Session Keys Reference](#session-keys-reference)
15. [Known Gaps & Incomplete Features](#known-gaps--incomplete-features)

---

## Architecture Overview

```
run.py                         → Flask entry point (localhost:5000, auto-opens Safari on macOS)
config.py                      → API keys (from .env), all directory paths, auto-creates dirs
app/__init__.py                → Flask app factory (hardcoded secret_key="dev-secret-change-me")
app/routes.py                  → All HTTP routes (~2090 lines, single monolithic file)
app/services/
  ├── leadspicker_api.py       → Leadspicker HTTP client (CSRF scraping, project listing, people fetch, person push)
  ├── leadspicker_ingest.py    → LP normalization, master schema alignment, dedup, master log append
  ├── airtable_api.py          → Airtable/Crunchbase client (fetch, batch update, batch create)
  ├── news_api.py              → NewsAPI.org client (fetch everything endpoint)
  ├── news_ingest.py           → News article normalization to standard schema
  ├── io_csv.py                → All CSV I/O, label normalization, labeling memory management
  └── logic_master.py          → Empty placeholder (no code)
app/templates/
  ├── index.html               → Main menu (4 buttons: Leadspicker, Crunchbase, News, Other)
  ├── about.html               → Static about page
  ├── other.html               → Placeholder "Other Sources" page
  ├── crunchbase.html           → Crunchbase import/fetch menu
  ├── crunchbase_analyze.html   → Crunchbase one-record-at-a-time analysis UI
  ├── news.html                 → News fetch/upload page
  └── leadspicker/
      ├── menu.html            → LP sub-menu (General vs Czech pipeline selector)
      ├── leadspicker.html     → LP import/fetch/dedup/push menu (shared by both pipelines via lp_mode)
      ├── analyze.html         → LP one-record-at-a-time labeling UI (shared by both pipelines via lp_mode)
      ├── draft_messages.html  → LP message drafting UI (shared by both pipelines)
      ├── draft_edit.html      → LP bulk draft row removal table
      └── done.html            → LP analysis completion summary (currently unused/legacy)
```

---

## Technology Stack & Dependencies

| Component | Technology |
|-----------|-----------|
| Backend | Python 3, Flask |
| Templating | Jinja2 (Flask default) |
| Data manipulation | pandas |
| Leadspicker API | `requests`, `beautifulsoup4` (CSRF scraping) |
| Airtable API | `pyairtable`, `requests` |
| News API | `requests` (newsapi.org v2) |
| HTML sanitization | `markupsafe` (for keyword highlighting) |
| Environment | `python-dotenv` |
| Data format | CSV (semicolon-delimited, UTF-8) |

---

## Configuration & Environment

**File**: `config.py`

### Environment Variables (from `.env`)

| Variable | Purpose |
|----------|---------|
| `AIRTABLE_API_KEY` | Airtable PAT (Personal Access Token) with schema.bases:read + data:read/write |
| `LEADSPICKER_API_KEY` | Leadspicker API key (used in X-API-Key header) |
| `LEADSPICKER_CSRFTOKEN` | Leadspicker CSRF token (defined but not actively used; CSRF is scraped dynamically) |
| `NEWS_API_KEY` | newsapi.org API key |
| `FLASK_DEBUG` | Optional; defaults to "1" (debug mode) |

### Flask Config

- `secret_key`: Hardcoded as `"dev-secret-change-me"` (used for Flask session cookies)
- `host`: `127.0.0.1`, `port`: `5000`
- Debug mode enabled by default

---

## Data Storage Model

All data is persisted as **local filesystem CSVs** using semicolon (`;`) delimiter, UTF-8 encoding, with atomic writes (write to `.tmp`, then `os.replace`).

### Directory Structure

```
data/
├── master/
│   └── master_log.csv                          # Authoritative record of all contacted leads
├── leadspicker/
│   ├── expansion_general_post/                 # General LP pipeline
│   │   ├── fetches/                            # Raw API responses & manual uploads
│   │   ├── normalized_fetches/                 # Mapped to master-like schema
│   │   ├── dropped_duplicates/                 # After dedup against labeling memory
│   │   ├── analyzed/                           # Analysis CSVs (subset of columns for review)
│   │   ├── drafted_messages/                   # Relevant rows + message_draft column
│   │   └── labeling_memory.csv                 # Cumulative record of all reviewed posts
│   └── expansion_czechia_post/                 # Czech LP pipeline (same subfolder structure)
│       ├── fetches/
│       ├── normalized_fetches/
│       ├── dropped_duplicates/
│       ├── analyzed/
│       ├── drafted_messages/
│       └── labeling_memory.csv
├── airtable/
│   ├── fetches/                                # Airtable fetch results & manual uploads
│   ├── analyzed/                               # Crunchbase analysis CSVs
│   └── labeling_memory.csv                     # CB labeling memory (exists but not actively used)
├── news/
│   ├── fetches/                                # Raw news API responses & manual uploads
│   ├── normalized/                             # Normalized to NEWS_COLUMNS schema
│   ├── analyzed/                               # (Directory exists but not wired to routes)
│   ├── drafted_messages/                       # (Directory exists but not wired to routes)
│   └── labeling_memory.csv                     # (File path defined but not wired to routes)
├── backups/                                    # Legacy placeholder (not auto-created, not used)
├── imports/                                    # Legacy placeholder
├── exports/                                    # Legacy placeholder
└── staging/                                    # Legacy placeholder
```

### CSV Conventions (io_csv.py)

- **Delimiter**: `;` (semicolon)
- **Encoding**: UTF-8
- **NaN handling**: `keep_default_na=False`, `na_filter=False` — empty strings stay as `""`, never converted to NaN
- **Write method**: Atomic — writes to `path.tmp` first, then `os.replace(tmp, path)`
- **Bad lines**: Skipped (`on_bad_lines="skip"`)
- **Label normalization**: Applied on every read/write of labeling and analysis files:
  - `y/yes/true/1` → `y`
  - `n/no/false/0` → `n`
  - `cc` → `CC` (Compliance Checkpoint, Czech pipeline only)

---

## Master Schema

**File**: `leadspicker_ingest.py` → `MASTER_COLUMNS` (73 columns)

The master log is the authoritative record of all leads across all pipelines. Its columns fall into groups:

### IDs & Routing
| Column | Description |
|--------|-------------|
| `id` | Sequential global ID (format: `L00001`) |
| `source_based_id` | Sequential source-specific ID (format: `LP00001`) |
| `global_fingerprint` | Deterministic dedup key: `company_name_norm\|domain_norm` |
| `source_batch_id` | Batch identifier (format: `LP-DD-MM-YYYY`) |
| `global_source` | Source system: `"LP"` for Leadspicker |

### Global Identity (Company)
| Column | Description |
|--------|-------------|
| `global_company_name_raw` | Company name as-is |
| `global_company_name_norm` | Lowercased, trimmed company name |
| `global_domain_norm` | Extracted domain (protocol/www/trailing slash stripped) |

### Global Identity (Lead/Contact)
| Column | Description |
|--------|-------------|
| `global_first_name` | Contact first name |
| `global_last_name` | Contact last name |
| `global_linkedin_url` | Contact's LinkedIn profile URL |
| `global_relation_to_the_company` | Job title / role / position |

### Global Workflow Status
| Column | Description |
|--------|-------------|
| `global_status` | Workflow state: `"new"`, `"contacted"` |
| `global_first_seen_at` | Date first imported |
| `global_last_seen_at` | Date last seen/updated |
| `global_analyzed_at` | Date analyzed/labeled |
| `global_message_drafted_at` | Date outreach message drafted |
| `global_pushed_airtable_at` | Date pushed to Airtable |
| `global_pushed_leadspicker_at` | Date pushed to Leadspicker |
| `global_responded` | Whether lead responded |
| `global_responded_channel` | Response channel (LinkedIn, email, etc.) |
| `global_responded_at` | Response date |
| `global_response_sentiment` | Sentiment of response |
| `global_no_linkedin_messages` | Count of LinkedIn messages sent |
| `global_no_email_messages` | Count of email messages sent |
| `global_airtable_id` | Airtable record ID |
| `global_bounced` | Whether email bounced |
| `global_leadspicker_project` | LP project ID this lead was pushed to |
| `global_leadspicker_contact_id` | LP contact ID |
| `global_email_subject` | Email subject line |
| `email_message_draft` | Final email message text |

### Crunchbase Fields (28 columns, prefixed `cb_`)
All empty for LP-sourced leads. Populated for Crunchbase-sourced leads:
`cb_company_name`, `cb_status`, `cb_reviewed_by_roman`, `cb_message_ready`, `cb_created`, `cb_updated`, `cb_tag`, `cb_crunchbase_profile`, `cb_financials_link`, `cb_people_link`, `cb_company_website`, `cb_series`, `cb_last_funding_date`, `cb_industries`, `cb_number_of_employees`, `cb_number_of_investors`, `cb_last_funding_amount_in_usd`, `cb_number_of_funding_rounds`, `cb_hq`, `cb_founded_on`, `cb_revenue_range`, `cb_company_email`, `cb_main_contact`, `cb_secondary_contact_1`, `cb_secondary_contact_2`, `cb_secondary_contact_3`, `cb_company_linkedin`, `cb_description`, `cb_investors`

### Leadspicker Fields (prefixed `lp_`)
| Column | Description |
|--------|-------------|
| `lp_company_name` | Company name from LP |
| `lp_status` | LP-specific status |
| `lp_relation_to_the_company` | Position/role from LP |
| `lp_replied` | Whether replied in LP |
| `lp_left_out` | LP's is_left_out flag |
| `lp_company_linkedin` | Company LinkedIn URL (raw) |
| `lp_company_linkedin_cleaned` | Company LinkedIn URL (trimmed at 5th `/`) |
| `lp_company_linkedin_expand` | `{cleaned_url}/people/?keywords=expand` |
| `lp_company_linkedin_expansion` | `{cleaned_url}/people/?keywords=expansion` |
| `lp_company_linkedin_partnership` | `{cleaned_url}/people/?keywords=partnership` |
| `lp_company_linkedin_ceo` | `{cleaned_url}/people/?keywords=ceo` |
| `lp_comapny_linkedin_czech` | `{cleaned_url}/people/?keywords=czech` (note: typo in column name) |
| `lp_company_linkedin_slovak` | `{cleaned_url}/people/?keywords=slovak` |
| `lp_slovak_linkedin_prague` | `{cleaned_url}/people/?keywords=prague` |
| `lp_company_website` | Company website from LP |
| `lp_country` | Country from LP |
| `lp_created_at` | LP record creation date |
| `lp_lead_first_name` | Lead first name from LP |
| `lp_lead_last_name` | Lead last name from LP |
| `lp_email` | Lead email from LP |
| `lp_contacted_lead_linkedin` | Lead's LinkedIn from LP |
| `lp_linkedin_post` | Full text of the LinkedIn post |
| `lp_base_post_url` | URL of the LinkedIn post (primary dedup key) |
| `lp_summary` | LP AI summary |
| `lp_source_robot` | LP source robot identifier |

### Analysis-Only Columns (not in master, used in analysis CSVs)
| Column | Description |
|--------|-------------|
| `relevant` | User label: `y`, `n`, `CC`, or empty |
| `is_really_relevant(AI)` | Placeholder for future AI quality review |
| `learning_data` | Flag for ML training data: `y` or empty |
| `lp_lead_full_name` | Computed: `first_name + " " + last_name` |
| `lp_classifier` | LP AI classifier value (in normalized data, not in analysis) |

---

## Pipeline 1: Leadspicker (General)

### Routes

| Route | Method | Function | Purpose |
|-------|--------|----------|---------|
| `/leadspicker/menu` | GET | `leadspicker_menu()` | Sub-menu: General vs Czech pipeline selector |
| `/leadspicker` | GET, POST | `leadspicker()` | Main import/dedup/push dashboard |
| `/leadspicker/analyze` | GET, POST | `leadspicker_analyze()` | One-record-at-a-time labeling/enrichment UI |
| `/leadspicker/drafts` | GET, POST | `leadspicker_draft_messages()` | Message drafting UI |
| `/leadspicker/drafts/edit` | GET, POST | `leadspicker_draft_edit()` | Bulk row removal from draft batch |

### Step-by-Step Flow

#### Step 1: Import (Fetch or Upload)

**Action `list_projects`**:
1. Calls `leadspicker_api.get_session_and_csrf()` — establishes HTTP session, scrapes CSRF token from LP API docs page using 4 fallback methods: `body[data-csrf-token]`, `<meta name="csrf-token">`, hidden input `csrfmiddlewaretoken`, cookies (`csrftoken`, `CSRF-TOKEN`, `XSRF-TOKEN`, `csrf`)
2. Calls `leadspicker_api.get_project_ids(session, csrf)` — GET `/app/sb/api/projects` with `X-API-Key` and `X-CSRFToken` headers
3. Returns list of `{"id": int, "name": str}` dicts displayed in the UI

**Action `api_fetch`**:
1. Takes comma-separated project IDs from form (default: `"11870"`)
2. Calls `leadspicker_ingest.fetch_api(project_ids, since_date="")` which:
   - Gets session + CSRF
   - For each project ID, calls `leadspicker_api.get_project_info(session, csrf, project_id)` — paginated GET to `/app/sb/api/projects/{id}/people?page={n}&page_size=50` until 400 or empty
   - Flattens nested JSON via `pd.json_normalize(items)`
   - Concatenates all project frames
3. Saves raw CSV to `fetches/LP_{timestamp}_raw.csv`
4. Normalizes via `leadspicker_ingest.normalize_to_master_like(df_raw, source_batch_id)`:
   - Maps LP's nested `contact_data.*.value` fields to flat `global_*` and `lp_*` columns
   - Extracts domain from website URL (strips protocol, www, trailing slash)
   - Generates 7 LinkedIn people-search URLs (expand, expansion, partnership, ceo, czech, slovak, prague)
   - Trims company LinkedIn URLs at the 5th `/` character
   - Sets `global_status="new"`, dates to today, empty placeholders for all CB and workflow fields
5. Aligns to master schema (ensures all 73 columns exist in order, keeps extras)
6. Saves normalized CSV to `normalized_fetches/LP_{timestamp}_normalized.csv`
7. Stores path in `session["lp_current_stage"]`

**Action `manual`**:
1. User uploads a CSV file
2. Saved to `fetches/LP_{timestamp}_raw.csv`
3. Loaded via `leadspicker_ingest.load_manual_csv()` (UTF-8, `;` delimiter)
4. Same normalize → align → save flow as API fetch

#### Step 2: Deduplicate

**Action `drop_dupes`**:
1. Reads staged normalized CSV from `session["lp_current_stage"]`
2. Reads labeling memory from `labeling_memory.csv` via `io_csv.read_lp_labeling_memory()`
3. Calls `leadspicker_ingest.drop_dupes_against_lp_memory(df_stage, df_mem)`:
   - Normalizes `lp_base_post_url` in both DataFrames (lowercase, strip protocol/www, trailing slash, spaces)
   - Removes rows whose normalized URL exists in memory
   - Returns `(filtered_df, stats)` with counts: incoming, dropped, remaining
4. Saves deduped CSV to `dropped_duplicates/{stem}_deduped.csv`
5. Creates analysis file via `leadspicker_ingest.save_lp_analysis_from_df(df_dedup)`:
   - Ensures `relevant` and `learning_data` columns exist
   - Saves to `analyzed/lp_labeling_analysis_{timestamp}.csv`
6. Stores analysis path in `session["lp_analysis_path"]`

#### Step 3: Analyze / Label

**Action `start_analysis`** or **`load_analysis`**: Redirects to `/leadspicker/analyze`

The analysis page operates on one record at a time with session-based row index tracking.

**Initialization**:
- Loads analysis CSV via `io_csv.read_lp_analysis(path)` (normalizes labels on load)
- Computes label counts: `y`, `n`, `unlabeled`
- If no row index stored, auto-navigates to first unlabeled row (wraps around)
- Loads master log DataFrame into module-level cache `_MASTER_DF` for search

**Labeling Actions**:

| Action | Effect |
|--------|--------|
| `label_y` (YES) | Sets `relevant=y`, opens enrichment panel, stays on current row |
| `label_n` (NO) | Sets `relevant=n`, closes enrichment, auto-advances to next unlabeled row |
| `label_n_learning` (NO + Learning Data) | Sets `relevant=n` AND `learning_data=y`, auto-advances to next unlabeled |
| `skip` | No label change, advances to next unlabeled (or next row if all labeled) |
| `prev` | Navigate to previous row |

**Enrichment Panel** (opened on YES):

When YES is pressed, a form panel appears to fill in / correct contact details:
- **Full Name** → split into first/last via `_split_full_name()` (handles "Last, First" format)
- **LinkedIn URL** (person)
- **Relation to Company** (job title)
- **Company Name**
- **Company Website**
- **Company LinkedIn** → also saved to `lp_company_linkedin` and `lp_company_linkedin_cleaned` (trailing `/` stripped)

Two save modes:
- `save_enrich` — saves enrichment, closes panel, advances to next row
- `save_enrich_stay` — saves enrichment, keeps panel open on same row

**Master Search** (inline company lookup):

| Action | Effect |
|--------|--------|
| `master_search` | Takes query, stores in `session["lp_master_query"]`, reloads page |

Search logic:
1. Normalizes query via `_norm_company_name()` (lowercase, collapse whitespace)
2. **Exact match**: `global_company_name_norm == normalized_query` (up to 10 results)
3. **Partial match**: `global_company_name_norm.contains(query)` OR `global_company_name_raw.contains(query, case=False)`, excluding exact matches (up to 10 results)
4. Displayed columns: `global_company_name_raw`, `global_company_name_norm`, `global_domain_norm`, `global_status`, `global_first_seen_at`, `global_last_seen_at`, `global_leadspicker_project`, `global_leadspicker_contact_id`
5. Search bar is cleared on any label/navigation action

**Finish Labeling** (`finish_labeling`):
1. Saves current analysis file
2. Filters rows where `relevant` is `y`, `n`, or `CC`
3. Appends labeled rows to `labeling_memory.csv` (deduplicates by `lp_base_post_url`, keeping first occurrence)
4. Redirects back to main Leadspicker page

**Keyword Highlighting**:

Two implementations exist:
1. **Python-side** (`highlight_keywords()` in routes.py): Used in template rendering, wraps matches in `<mark>` tags
2. **Jinja filter** (`highlight_lp_keywords`): Registered as `@app.template_filter`, wraps matches in `<span class="hl">` tags

Keywords highlighted (regex, case-insensitive):
- expand/expanding/expanded/expands, expansion/expansions
- scale/scaling/scaled/scales
- grow/grows/grew/grown/growing/growth
- global/globally, worldwide, overseas, abroad
- europe/european
- international/internationally, cross-border
- (filter only) czech/czechia/czech republic, enter/enters/entered/entering, launch/launches/launched/launching

#### Step 4: Draft Messages

**Action `open_draft_messages`** (from main LP page):
1. Takes analyzed file path (from form or session)
2. Filters to rows where `relevant == "y"` only
3. Adds `message_draft` column if missing
4. Saves to new file: `drafted_messages/relevant_messages_drafted_{timestamp}.csv`
5. Stores path in `session["lp_drafts_path"]`
6. Redirects to `/leadspicker/drafts`

**Draft Messages UI** (`/leadspicker/drafts`):
- Filters out rows where `remove_from_drafting == "y"`
- Displays one record at a time showing:
  - **Author info**: name (global or LP fallback), LinkedIn URL, relation to company
  - **Company info**: name, LinkedIn URL, website/domain
  - **Post**: full LinkedIn post text, post URL
  - **Message draft**: editable textarea
  - **Contextual statements**:
    - "This is the author of the post" vs conflict warning (if global name ≠ LP name)
    - "The post is written by a person from the company" vs "outsider" (if global company ≠ LP company)

**Draft Actions**:

| Action | Effect |
|--------|--------|
| `save_draft` | Auto-saves draft text; if row marked for removal, drops it from DataFrame and saves |
| `prev` / `next` | Navigate between records (auto-saves draft text) |
| `finish_drafting` | Drops all rows marked for removal, saves file, redirects to LP page (or Czech page if file is under Czech dir) |
| `open_enrich` | Opens enrichment panel on current draft record |
| `close_enrich` | Closes enrichment panel |
| `save_enrich` | Saves enrichment fields (same fields as analyze enrichment), closes panel |

**Draft text is auto-saved** on every action (even navigation) to prevent losing work.

**Inline Enrichment on Drafts**: Same fields as analysis enrichment (full name, first/last name override, LinkedIn URL, relation, company name, website, company LinkedIn).

#### Step 5: Edit Drafted Records

**Route**: `/leadspicker/drafts/edit` (`leadspicker_draft_edit()`)
- Table view of all rows in the draft file
- Each row shows: name, position, company, is-author flag, is-from-company flag, post text preview
- Checkboxes to mark rows for removal (`remove_from_drafting`)
- "Save" persists selections, "Back" returns to draft messages view

#### Step 6: Push

**Action `push_drafted_wip`** (from main LP page):
1. Takes drafted file name and LP project ID from form
2. Reads drafted CSV
3. Calls `leadspicker_api.push_drafted_dataframe(df, project_id, limit=50)`:
   - **Limit**: Capped at 50 rows (WIP safety measure)
   - For each row, builds JSON payload via `build_person_payload(row, project_id)`:
     - Top-level fields: `first_name`, `last_name`, `linkedin`, `position`, `company_name`, `company_website`, `company_linkedin`, `email`, `country`, `salesnav`
     - Custom fields: `base_post_url`, `Message - desc` (draft message), `linkedin_post`, `email_subject`
     - `project_id`, `data_source: "user_provided"`
   - POSTs to `https://app.leadspicker.com/app/sb/api/persons` with `x-api-key` header
   - Collects per-row results (status, ok/fail, response text)

4. **Post-push side effects** (only for successfully pushed rows):

   a. **Labeling memory update**: Appends rows with `relevant` in {y, n} to `labeling_memory.csv`, deduplicates by `lp_base_post_url`

   b. **Master log append** via `leadspicker_ingest.append_lp_rows_to_master(df, project_id)`:
      - Aligns to master schema
      - Sets `global_source="LP"`, `global_status="contacted"`, dates to today
      - Copies `message_draft` → `email_message_draft`
      - Generates fingerprint if missing
      - Deduplicates against existing master by `global_leadspicker_contact_id` and normalized `lp_base_post_url`
      - Assigns sequential IDs: `L{next_seq:05d}` and `LP{next_seq:05d}`
      - Appends to `master_log.csv` atomically

   c. **Airtable record creation** via `airtable_api.create_leadspicker_general_post_records(df)`:
      - Creates records in Airtable table "Leadspicker - general post"
      - Field mapping (`LP_GENERAL_POST_FIELD_MAP`):
        | Airtable Field | Source |
        |----------------|--------|
        | Company Name | `global_company_name_raw` |
        | First Name | `global_first_name` |
        | Full Name | computed: `first + " " + last` |
        | Last Name | `global_last_name` |
        | Created | `global_pushed_leadspicker_at` |
        | leadspicker_id | `global_leadspicker_contact_id` |
        | Contact LinkedIn profile | `global_linkedin_url` |
        | Base post URL | `lp_base_post_url` |
        | General message | `message_draft` |
        | Base post | `lp_linkedin_post` |
        | Company LinkedIn URL | `global_company_linkedin` |
        | Relation to the company | `global_relation_to_the_company` |
        | status | `global_status` |
        | Company website | `global_domain_norm` |
      - Batch creates in chunks of 10 with `typecast=True`

   d. **Drafted file metadata update**: Sets `global_pushed_leadspicker_at`, `global_pushed_airtable_at` (if Airtable succeeded), `global_status="contacted"` on successfully pushed rows

---

## Pipeline 2: Leadspicker (Czech)

### Routes

| Route | Method | Function | Purpose |
|-------|--------|----------|---------|
| `/leadspicker/czech` | GET, POST | `leadspicker_czech()` | Czech import/dedup/push dashboard |
| `/leadspicker/czech/analyze` | GET, POST | `leadspicker_analyze_czech()` | Czech labeling/enrichment UI |

### Differences from General Pipeline

The Czech pipeline is a **parallel clone** of the General pipeline with the following differences:

| Aspect | General | Czech |
|--------|---------|-------|
| Data directory | `expansion_general_post/` | `expansion_czechia_post/` |
| Session key prefix | `lp_` | `lp_cz_` |
| Labeling memory | `io_csv.read/write_lp_labeling_memory()` | `io_csv.read/write_lp_czechia_labeling_memory()` |
| Extra label action | — | `label_cc` (Compliance Checkpoint): sets `relevant="CC"`, opens enrichment panel |
| Done values for skip | `{"y", "n"}` | `{"y", "n", "cc"}` (CC counts as "done") |
| Airtable table | "Leadspicker - general post" | "Leadspicker - czehcia post" (note: typo in code) |
| Airtable create function | `create_leadspicker_general_post_records()` | `create_leadspicker_czech_post_records()` |
| Enrichment session key | `lp_enrich_open` | `lp_cz_enrich_open` |
| Master search session key | `lp_master_query` | `lp_cz_master_query` |
| Template variable | `lp_mode="general"` | `lp_mode="czech"` |

### Czech Enrichment Specifics

The Czech pipeline's `save_enrich`/`save_enrich_stay` has a **pre-fill fallback** mechanism: if the user doesn't modify a field, it falls back to the original LP values (`lp_company_name`, `lp_company_linkedin`, etc.) rather than leaving the global field empty. The General pipeline does NOT have this fallback — it strictly uses form values.

### Shared Components

- Both pipelines share the **same templates**: `leadspicker.html`, `analyze.html`, `draft_messages.html`, `draft_edit.html` (differentiated by `lp_mode` variable)
- Both share the **same drafting routes**: `/leadspicker/drafts` and `/leadspicker/drafts/edit`
- The drafting route detects which pipeline the file belongs to via `_is_under(config.LP_CZECHIA_DRAFTED_DIR, path)` to know where to redirect after finishing

---

## Pipeline 3: Crunchbase / Airtable

### Routes

| Route | Method | Function | Purpose |
|-------|--------|----------|---------|
| `/crunchbase` | GET, POST | `crunchbase()` | CB import/fetch/push menu |
| `/crunchbase/analyze` | GET, POST | `crunchbase_analyze()` | CB one-record analysis UI |

### Airtable Configuration

- **Base ID**: `appSXOLAKJX3Vjo3n` (hardcoded)
- **Table**: "Crunchbase Source"
- **LP Target Base**: same base ID
- **LP Target Tables**: "Leadspicker - general post", "Leadspicker - czehcia post"

### Step-by-Step Flow

#### Step 1: Import

**Action `airtable_fetch` / `api_fetch`**:
1. Calls `airtable_api.fetch_crunchbase_source()` with optional filters:
   - `status_filter`: Airtable formula `{Status}='value'` (default: "Longlist")
   - `contact_enriched_zero`: Adds `{Contact enriched}=0` filter
   - `view`: Airtable view name
   - `max_records`: Row limit
   - Filters combined with `AND()` if multiple
2. Uses `pyairtable` library — `table.all()` with formula/view/max_records
3. Converts records to DataFrame via `records_to_df()`:
   - Extracts `fields` dict from each record
   - Normalizes `Message draft` field (if it's a `{state, value, isStale}` dict, extracts just `value`)
   - Preserves Airtable record `_id`
4. Saves to `fetches/CB_airtable_{timestamp}.csv`
5. Creates analysis file in `analyzed/cb_labeling_analysis_{timestamp}.csv`

**Action `manual`**: Upload CSV, same analysis creation flow.

**Action `load_stage`**: Load a previously saved fetch CSV.

#### Step 2: Analyze

**Actions**:

| Action | Effect |
|--------|--------|
| `label_y` (YES) | Sets `relevant=y`, advances to next unlabeled |
| `save_fields` (Save & Next) | Saves: Message fin, Main Contact, Secondary Contact #1/#2/#3; sets `Status="Quality B - Contacted"`; advances to next row |
| `save_fields_stay` (Save & Stay) | Same field saves but does NOT change Status; stays on current row |
| `eliminate` | Sets `Status="Eliminated"`, advances to next |
| `uneliminate` | Sets `Status="Longlist"`, stays on current row |
| `skip` / `prev` | Navigate without changes |
| `finish_labeling` | Saves analysis CSV, redirects to CB menu |

**Key difference from LP pipeline**: CB analysis has **no enrichment panel**. Instead it has editable fields directly: `Message fin` (outreach message), `Main Contact` (LinkedIn URL), `Secondary Contact #1`/`#2`/`#3` (LinkedIn URLs).

#### Step 3: Push Updates

**Action `push_updates`** (from CB menu):

Two checkboxes, either or both can be selected:

**a. Push to Airtable** (`push_airtable`):
- Calls `airtable_api.update_crunchbase_records(df)`:
  - Drops problematic columns that cause Airtable errors: `Half year reminder (suggested)`, `Message draft`, `CB financials link`, `CB people link`, `Contact enriched`, `CB news link`, `Tag`, `Tags`, `Reviewed by Roman`, `Number of Investors`, `relevant`, `learning_data`
  - Converts `Message fin` to string, replaces NaN with empty string
  - Batch updates in chunks of 10 via `table.batch_update(chunk, typecast=True)`
  - Matches records by `_id` (Airtable record ID)

**b. Push to Leadspicker** (`push_leadspicker`):
- Filters to rows where `Status == "Quality B - Contacted"` (case-insensitive)
- Maps CB fields to LP schema via `_cb_to_lp_df()`:
  - `Message fin` → `message_draft`
  - `Name` → `global_company_name_raw`
  - `Company LinkedIn` → `global_company_linkedin`
  - `Company Website` → `global_domain_norm`
  - **Contact selection**: Picks the *last non-empty* value from `[Main Contact, Secondary Contact #1, #2, #3]` → `global_linkedin_url`
- Drops rows with empty `message_draft`
- Pushes via `leadspicker_api.push_drafted_dataframe()` (same as LP pipeline)
- Appends successfully pushed rows to master log

---

## Pipeline 4: News

### Routes

| Route | Method | Function | Purpose |
|-------|--------|----------|---------|
| `/news` | GET, POST | `news()` | News fetch/upload page |

### Flow

#### Step 1: Fetch

**Action `api_fetch`**:
1. Calls `news_api.fetch_everything()`:
   - Endpoint: `https://newsapi.org/v2/everything`
   - Parameters: `q` (query), `from`, `to`, `language`, `sortBy=publishedAt`, `domains`, `pageSize=100`, `page=1`
   - Authorization via header
2. Default form values pre-filled:
   - **Query**: `("Series A" OR "Series B" OR "Series C") AND (expansion OR international expansion OR global expansion OR expand internationally OR expand globally)`
   - **Domains** (35 curated sources): techcrunch.com, news.crunchbase.com, venturebeat.com, theinformation.com, sifted.eu, siliconangle.com, geekwire.com, reuters.com, bloomberg.com, ft.com, startupgrind.com, eu-startups.com, startups.co, pitchbook.com, dealroom.co, tech.eu, thenextweb.com, techinasia.com, vccafe.com, strictlyvc.com, axios.com, forbes.com, fastcompany.com, businessinsider.com, techpoint.africa, african.business, startupdaily.net, yourstory.com, e27.co, thesaasnews.com, prnewswire.com, businesswire.com, globenewswire.com, einpresswire.com, accesswire.com, newswire.com
   - **Date range**: last 7 days
   - **Language**: en
3. Converts articles via `news_ingest.articles_to_df()`: extracts title, description, content, url, source.name, author, publishedAt, urlToImage
4. Normalizes via `news_ingest.normalize_articles()` into `NEWS_COLUMNS` schema:
   - `news_title`, `news_description`, `news_content`, `news_url`, `news_source`, `news_author`, `news_published_at`, `news_url_to_image`, `news_query`, `news_domains`, `news_language`, `news_fetched_at`
   - Uses flexible column matching (`_pick()` with case-insensitive fallback)
5. Saves raw to `fetches/NEWS_{timestamp}_raw.csv`, normalized to `normalized/NEWS_{timestamp}_normalized.csv`

**Action `manual`**: Upload CSV → normalize → save.

### Current Status

The news pipeline **only covers fetch + normalize**. The following are defined in config but **NOT wired into routes**:
- `NEWS_ANALYZED_DIR` — directory exists, no analysis route
- `NEWS_DRAFTED_DIR` — directory exists, no drafting route
- `NEWS_LABELING_MEMORY_FILE` — path defined, no dedup/memory route

---

## Cross-Cutting Functionality

### Master Log (`data/master/master_log.csv`)

- Authoritative record of all contacted leads across all pipelines
- Written atomically via `io_csv.write_csv_atomic()`
- Queried in-memory during analysis (cached in module-level `_MASTER_DF`)
- Dedup on append uses `global_leadspicker_contact_id` and normalized `lp_base_post_url`
- Sequential ID assignment: `L{n:05d}` for global ID, `LP{n:05d}` for source ID

### Labeling Memory (per-pipeline)

| Pipeline | File | Read Function | Write Function |
|----------|------|---------------|----------------|
| LP General | `expansion_general_post/labeling_memory.csv` | `io_csv.read_lp_labeling_memory()` | `io_csv.write_lp_labeling_memory()` |
| LP Czech | `expansion_czechia_post/labeling_memory.csv` | `io_csv.read_lp_czechia_labeling_memory()` | `io_csv.write_lp_czechia_labeling_memory()` |
| Crunchbase | `airtable/labeling_memory.csv` | `io_csv.read_cb_labeling_memory()` | `io_csv.write_cb_labeling_memory()` |

Updated at two points:
1. **"Finish Labeling"** in analyze UI — appends all labeled rows (y/n/CC)
2. **"Push"** — appends successfully pushed rows that have relevant labels

Dedup key: `lp_base_post_url` (for LP pipelines)

### `_split_full_name()` Helper

Splits "Full Name" into (first, last):
- "Last, First" → (First, Last) — comma-separated
- "First Middle Last" → (First, "Middle Last") — last token(s) as last name
- "Single" → (Single, "")

### `_is_under()` Helper

Security check: verifies a file path is under an expected base directory (prevents path traversal).

### `_first_unlabeled_index()` Helper

Finds next unlabeled row starting from a given index, wrapping around to the beginning. Configurable `done_values` set (default `{"y", "n"}`, Czech adds `"cc"`).

### Idempotent Memory Append (`io_csv.append_to_lp_memory_idempotent()`)

Defined but appears unused in current routes — a more robust version of labeling memory append that:
- Skips rows with empty dedup key
- Handles column union (preserves all columns from both existing and new data)
- Returns detailed stats

---

## Static / Placeholder Pages

| Route | Template | Content |
|-------|----------|---------|
| `/` | `index.html` | Main menu with 4 logo buttons: Leadspicker, Crunchbase, News, Other |
| `/about` | `about.html` | Static "About" page with back link |
| `/other` | `other.html` | Placeholder: "Uploads and scrapers coming soon" |

---

## Complete Route Map

| Route | Method | Function Name |
|-------|--------|---------------|
| `/` | GET | `index()` |
| `/leadspicker/menu` | GET | `leadspicker_menu()` |
| `/leadspicker` | GET, POST | `leadspicker()` |
| `/leadspicker/analyze` | GET, POST | `leadspicker_analyze()` |
| `/leadspicker/czech` | GET, POST | `leadspicker_czech()` |
| `/leadspicker/czech/analyze` | GET, POST | `leadspicker_analyze_czech()` |
| `/leadspicker/drafts` | GET, POST | `leadspicker_draft_messages()` |
| `/leadspicker/drafts/edit` | GET, POST | `leadspicker_draft_edit()` |
| `/crunchbase` | GET, POST | `crunchbase()` |
| `/crunchbase/analyze` | GET, POST | `crunchbase_analyze()` |
| `/news` | GET, POST | `news()` |
| `/other` | GET | `other()` |

---

## Complete Function & Helper Index

### routes.py

| Function | Purpose |
|----------|---------|
| `highlight_keywords(text)` | Wraps expansion keywords in `<mark>` tags (Python-side) |
| `highlight_lp_keywords(text)` | Jinja template filter, wraps keywords in `<span class="hl">` |
| `_is_under(base, target)` | Security: checks file is under expected directory |
| `_first_unlabeled_index(df, start, done_values)` | Finds next unlabeled row with wraparound |
| `_label_counts(df)` | Returns dict of y/n/unlabeled/total counts |
| `_list_drafted_files_for_dir(dir_path)` | Lists CSVs in a dir, sorted newest-first |
| `_list_drafted_files()` | Lists general pipeline drafted CSVs |
| `_list_cb_stage_files(limit)` | Lists CB fetch CSVs, newest-first |
| `_new_drafted_filename_for_dir(dir_path)` | Generates timestamped drafted filename |
| `_new_drafted_filename()` | Generates general pipeline drafted filename |
| `_cb_analysis_filename(ts)` | Generates CB analysis filename |
| `_cb_enrich_filename(ts)` | Generates CB enrichment filename (unused) |
| `_split_full_name(name)` | Splits "Full Name" into (first, last) |
| `_split_full_name_cb(name)` | Alias for `_split_full_name` |
| `_cb_to_lp_df(df)` | Maps CB columns to LP push schema |
| `_norm_company_name(s)` | Lowercase + collapse whitespace for company search |
| `_get_master_df(force)` | Loads/caches master log DataFrame |
| `_reset_master_search()` | Clears general pipeline search query from session |
| `_reset_master_search_cz()` | Clears Czech pipeline search query from session |

### leadspicker_api.py

| Function | Purpose |
|----------|---------|
| `get_session_and_csrf()` | Establishes LP HTTP session, scrapes CSRF token |
| `get_project_ids(session, csrf)` | Fetches all LP project IDs and names |
| `get_project_info(session, csrf, project_id)` | Fetches all people for a project (paginated) |
| `process_project_info(session, csrf, project_id)` | Convenience wrapper returning DataFrame |
| `build_person_payload(row, project_id)` | Builds JSON payload for POST /persons |
| `post_person(payload)` | POSTs a single person to LP API |
| `push_drafted_dataframe(df, project_id, limit)` | Pushes multiple rows, collects results |

### leadspicker_ingest.py

| Function | Purpose |
|----------|---------|
| `align_to_master_schema(df)` | Ensures all 73 master columns exist and are ordered |
| `build_analysis_from_stage(df_stage)` | Creates analysis-subset DataFrame from staged data |
| `_series_of_len(df, fill)` | Returns empty Series matching DataFrame length |
| `_pick(df, candidates)` | Returns first matching column (case-insensitive fallback) |
| `_norm_url(s)` | Normalizes URLs for matching |
| `fetch_api(project_ids, since_date)` | Full LP API fetch pipeline |
| `load_manual_csv(file_path)` | Loads manual CSV upload |
| `_trim_at_5th_slash_series(s)` | Trims URLs at 5th `/` character |
| `normalize_to_master_like(df_raw, source_batch_id)` | Maps LP API data to master schema |
| `drop_dupes_against_lp_memory(df_new, df_mem)` | Deduplicates by `lp_base_post_url` |
| `_lp_analysis_filename(ts, analyses_dir)` | Generates analysis filename |
| `save_lp_analysis_from_df(df, ts, analyses_dir)` | Saves analysis CSV |
| `save_lp_analysis_from_csv(staged_csv_path, ts)` | Loads CSV then saves as analysis |
| `dedupe_and_create_analysis(df, ts)` | Convenience: dedupe + create analysis in one call |
| `_next_sequence_value(series, prefix)` | Finds next sequential ID number |
| `append_lp_rows_to_master(df_rows, project_id)` | Appends drafted rows to master log |

### airtable_api.py

| Function | Purpose |
|----------|---------|
| `_get_api()` | Lazily instantiates pyairtable Api client |
| `get_table_ids(base_id)` | Fetches table names/IDs for a base (schema API) |
| `fetch_records(table_name, ...)` | Generic Airtable record fetch |
| `records_to_df(records, include_id)` | Converts Airtable records to DataFrame |
| `fetch_crunchbase_source(...)` | Convenience wrapper for CB Source table |
| `update_crunchbase_records(df, batch_size)` | Batch updates CB Source records |
| `create_leadspicker_general_post_records(df, ...)` | Batch creates in LP General table |
| `create_leadspicker_czech_post_records(df, ...)` | Batch creates in LP Czech table |
| `_create_airtable_records(table, df, field_map, batch_size)` | Internal batch create helper |

### io_csv.py

| Function | Purpose |
|----------|---------|
| `read_csv(path)` | Standard CSV read (`;` delimited, UTF-8, no NaN) |
| `write_csv_atomic(path, df)` | Atomic CSV write |
| `normalize_relevant_value(val)` | Single value: y/n/CC normalization |
| `normalize_yesno_value(val)` | Alias for `normalize_relevant_value` |
| `normalize_relevant_column(df, col)` | Column-wide label normalization |
| `normalize_yesno_column(df, col)` | Column-wide yes/no normalization |
| `ensure_analysis_has_columns(df)` | Ensures `relevant` and `learning_data` columns exist |
| `assert_key_present(df, key)` | Validates dedup key column exists |
| `read_lp_labeling_memory()` | Reads general LP labeling memory |
| `write_lp_labeling_memory(df)` | Writes general LP labeling memory |
| `read_lp_czechia_labeling_memory()` | Reads Czech LP labeling memory |
| `write_lp_czechia_labeling_memory(df)` | Writes Czech LP labeling memory |
| `read_cb_labeling_memory()` | Reads CB labeling memory |
| `write_cb_labeling_memory(df)` | Writes CB labeling memory |
| `append_to_lp_memory_idempotent(df_new, key)` | Idempotent append with stats (defined but unused) |
| `list_lp_analysis_files(limit)` | Lists LP analysis CSVs, newest-first |
| `read_lp_analysis(path)` | Reads + normalizes LP analysis CSV |
| `write_lp_analysis(path, df)` | Writes + normalizes LP analysis CSV |
| `list_cb_analysis_files(limit)` | Lists CB analysis CSVs, newest-first |
| `read_cb_analysis(path)` | Reads + normalizes CB analysis CSV |
| `write_cb_analysis(path, df)` | Writes + normalizes CB analysis CSV |

### news_api.py

| Function | Purpose |
|----------|---------|
| `fetch_everything(query, ...)` | Fetches from NewsAPI.org `/v2/everything` |

### news_ingest.py

| Function | Purpose |
|----------|---------|
| `_series_of_len(df, fill)` | Returns empty Series matching DataFrame length |
| `_pick(df, candidates)` | Returns first matching column |
| `align_to_news_schema(df)` | Ensures all NEWS_COLUMNS exist and are ordered |
| `articles_to_df(articles)` | Converts API response articles to DataFrame |
| `normalize_articles(df, ...)` | Normalizes articles to NEWS_COLUMNS schema |
| `load_manual_csv(path)` | Loads manual CSV upload for news |

### logic_master.py

**Empty file** — placeholder for future master log logic.

---

## Session Keys Reference

### Leadspicker General

| Key | Type | Purpose |
|-----|------|---------|
| `lp_current_stage` | str (path) | Path to current normalized/deduped CSV |
| `lp_ready_to_analyze` | bool | Whether dedup is done and analysis is ready |
| `lp_analyze_idx` | int | Legacy analysis index |
| `lp_analysis_file` | str | Legacy analysis file reference |
| `lp_analysis_path` | str (path) | Path to current analysis CSV |
| `lp_analysis_row_idx` | int | Current row index in analyze UI |
| `lp_enrich_open` | bool | Whether enrichment panel is visible |
| `lp_master_query` | str | Current master search query |
| `lp_drafts_path` | str (path) | Path to current drafted messages CSV |
| `lp_drafts_row_idx` | int | Current row index in drafts UI |
| `lp_drafts_enrich_open` | bool | Whether enrichment panel is open in drafts |

### Leadspicker Czech

Same keys with `lp_cz_` prefix:
`lp_cz_current_stage`, `lp_cz_ready_to_analyze`, `lp_cz_analyze_idx`, `lp_cz_analysis_file`, `lp_cz_analysis_path`, `lp_cz_analysis_row_idx`, `lp_cz_enrich_open`, `lp_cz_master_query`

### Crunchbase

| Key | Type | Purpose |
|-----|------|---------|
| `cb_current_stage` | str (path) | Path to current staged CSV |
| `cb_analysis_path` | str (path) | Path to current CB analysis CSV |
| `cb_analysis_row_idx` | int | Current row index in CB analyze UI |
| `cb_ready_to_analyze` | bool | Whether analysis is ready |

---

## Known Gaps & Incomplete Features

1. **News pipeline**: Only fetch + normalize implemented. No analysis, labeling, drafting, or push routes exist despite directories being created.

2. **`logic_master.py`**: Empty file — placeholder for future master log business logic.

3. **`other.html` route**: Static placeholder page ("Uploads and scrapers coming soon").

4. **`done.html` template**: LP analysis completion summary page exists but is not referenced by any current route — likely legacy or planned.

5. **`_cb_enrich_filename()`**: Defined in routes.py but never called. References `config.CB_ENRICH_DIR` which does not exist in config.py (would crash if called).

6. **Push limit**: LP push is capped at 50 rows (`limit=50`) as a WIP safety measure.

7. **Secret key**: Hardcoded `"dev-secret-change-me"` — not production-ready.

8. **CSRF token from env**: `LEADSPICKER_CSRFTOKEN` is loaded from env but never used — CSRF is always scraped dynamically.

9. **Column name typo**: `lp_comapny_linkedin_czech` (missing 'p' in "company") in master schema.

10. **Airtable table name typo**: `LP_TABLE_NAME_CZECH = "Leadspicker - czehcia post"` (should be "czechia").

11. **`append_to_lp_memory_idempotent()`**: Defined in io_csv.py but never called by any route.

12. **`get_table_ids()`**: Defined in airtable_api.py but never called by any route.

13. **`build_analysis_from_stage()`**: Imported in routes.py but only used implicitly through `save_lp_analysis_from_df()`.

14. **`lp_classifier`**: Extracted during normalization from `contact_data.ai_classifier.value` but not included in `ANALYSIS_COLUMNS`, so it's lost when the analysis file is created.

15. **CB labeling memory**: Functions exist (`read_cb_labeling_memory`, `write_cb_labeling_memory`) but are never called in any route.

16. **No pagination for news**: Only fetches page 1 (up to 100 articles).

17. **No about route**: `about.html` template exists but no route serves it.
