# USL Signal Hunter — Product Requirements Document (PRD)

> **Version**: 1.0 (baseline from MVP)
> **Status**: Draft — capturing current MVP functionality as requirements for the production build
> **Last updated**: 2026-03-04

---

## 1. Product Overview

### 1.1 Purpose

USL Signal Hunter is an **internal lead-processing tool** for a sales/business development team. It consolidates multiple lead sources into a single workflow where operators can discover signals (LinkedIn posts, Crunchbase companies, news articles), qualify them, enrich contact data, draft personalized outreach messages, and push finalized leads to external systems (Leadspicker CRM, Airtable tracking).

### 1.2 Problem Statement

The team sources leads from multiple platforms (Leadspicker, Crunchbase/Airtable, news outlets). Without a unified tool, the workflow involves:
- Manually downloading CSVs and switching between browser tabs
- No deduplication — the same lead gets processed multiple times
- No centralized record of who was contacted, when, and through which channel
- Drafting messages in separate documents with no connection to the lead data
- Manual copy-paste to push contacts back into CRM and tracking systems

### 1.3 Target Users

Internal operators (1–5 people) on a business development / partnerships team. Not customer-facing. Single-user sessions expected (no concurrent editing concerns in MVP).

### 1.4 Core Value Proposition

One tool, one flow: **Import → Deduplicate → Analyze/Label → Enrich → Draft → Push** — with a persistent master log that ensures no lead is contacted twice and no work is lost.

---

## 2. User Roles & Access

### 2.1 Current State (MVP)

- **Single role**: Operator. No authentication, no authorization.
- **Single session**: Flask session cookie with hardcoded secret key.
- **Local deployment**: Runs on `localhost:5000`, accessed from the same machine.

### 2.2 Requirements for Production Build

| Req ID | Requirement | Priority |
|--------|-------------|----------|
| AUTH-1 | The system must support user authentication (login/logout) | TBD |
| AUTH-2 | The system must support at minimum an "operator" role | TBD |
| AUTH-3 | Session secrets must be configurable via environment, not hardcoded | Must |

---

## 3. Data Sources (Ingest Pipelines)

The system ingests leads from three external sources, each with its own import mechanism. A fourth ("Other") is reserved for future sources.

### 3.1 Leadspicker

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| SRC-LP-1 | The system must authenticate with the Leadspicker API by establishing an HTTP session and obtaining a CSRF token | Done |
| SRC-LP-2 | CSRF token acquisition must try multiple extraction methods in order: `body[data-csrf-token]`, `<meta name="csrf-token">`, hidden `csrfmiddlewaretoken` input, cookie names (`csrftoken`, `CSRF-TOKEN`, `XSRF-TOKEN`, `csrf`) | Done |
| SRC-LP-3 | The system must list all available Leadspicker projects (ID + name) | Done |
| SRC-LP-4 | The system must fetch all "people" records for one or more project IDs, handling pagination automatically (page_size=50, stop on HTTP 400 or empty page) | Done |
| SRC-LP-5 | The system must support manual CSV upload as an alternative to API fetch | Done |
| SRC-LP-6 | Raw fetched data must be saved to a `fetches/` directory before any transformation | Done |
| SRC-LP-7 | The system must support two independent Leadspicker pipelines: **General** (expansion_general_post) and **Czech** (expansion_czechia_post) with separate data directories and labeling memories | Done |

### 3.2 Crunchbase (via Airtable)

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| SRC-CB-1 | The system must fetch records from an Airtable base/table ("Crunchbase Source") using the pyairtable library | Done |
| SRC-CB-2 | Fetch must support filtering by: Status field value, Contact enriched=0, Airtable view name, max record count | Done |
| SRC-CB-3 | Multiple filters must be combined with Airtable's `AND()` formula | Done |
| SRC-CB-4 | The system must preserve Airtable record IDs (`_id`) for later batch updates | Done |
| SRC-CB-5 | The `Message draft` field must be normalized — if it's a `{state, value, isStale}` object, extract just `value` | Done |
| SRC-CB-6 | The system must support manual CSV upload as an alternative to Airtable fetch | Done |
| SRC-CB-7 | The system must support loading previously saved fetch files from the staging directory | Done |

### 3.3 News (via NewsAPI.org)

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| SRC-NEWS-1 | The system must fetch articles from NewsAPI.org `/v2/everything` endpoint | Done |
| SRC-NEWS-2 | Fetch must support configurable: search query, date range (from/to), language, domain whitelist | Done |
| SRC-NEWS-3 | The system must provide sensible default values: a funding/expansion-focused query, 35 curated news domains, 7-day date range, English language | Done |
| SRC-NEWS-4 | The system must support manual CSV upload as an alternative to API fetch | Done |
| SRC-NEWS-5 | The system must normalize articles to a standard schema: `news_title`, `news_description`, `news_content`, `news_url`, `news_source`, `news_author`, `news_published_at`, `news_url_to_image`, `news_query`, `news_domains`, `news_language`, `news_fetched_at` | Done |
| SRC-NEWS-6 | Column matching during normalization must be flexible (case-insensitive fallback, multiple candidate column names) | Done |
| SRC-NEWS-7 | **The news pipeline must have a downstream analysis/labeling/drafting/push flow** | Not done — fetch + normalize only |

### 3.4 Other Sources

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| SRC-OTHER-1 | The system must have a placeholder/extensibility point for additional lead sources | Placeholder page only |

---

## 4. Data Normalization

### 4.1 Leadspicker Normalization

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| NORM-LP-1 | Raw LP API data (nested `contact_data.*.value` fields) must be mapped to a flat master-like schema with `global_*` and `lp_*` prefixed columns | Done |
| NORM-LP-2 | Company website must be normalized to a bare domain (strip protocol, www, trailing slash) | Done |
| NORM-LP-3 | Company LinkedIn URL must be trimmed at the 5th `/` character to get the base profile URL | Done |
| NORM-LP-4 | The system must generate LinkedIn people-search convenience URLs using the cleaned company LinkedIn as base: keywords = `expand`, `expansion`, `partnership`, `ceo`, `czech`, `slovak`, `prague` | Done |
| NORM-LP-5 | A deterministic fingerprint must be generated: `company_name_norm|domain_norm` | Done |
| NORM-LP-6 | Company name normalization: lowercase + trim | Done |
| NORM-LP-7 | All records must be initialized with `global_status="new"` and today's date for `first_seen_at` and `last_seen_at` | Done |

### 4.2 News Normalization

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| NORM-NEWS-1 | Articles must be normalized to the `NEWS_COLUMNS` schema | Done |
| NORM-NEWS-2 | Column matching must try multiple candidate names per field (e.g., `title`, `headline` both map to `news_title`) | Done |
| NORM-NEWS-3 | Metadata columns (`news_query`, `news_domains`, `news_language`, `news_fetched_at`) must be attached to each row | Done |

---

## 5. Deduplication

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| DEDUP-1 | The system must maintain a **labeling memory** file per pipeline that records all previously reviewed leads | Done |
| DEDUP-2 | LP deduplication must be based on normalized `lp_base_post_url` (lowercase, strip protocol/www/trailing slash/spaces) | Done |
| DEDUP-3 | Deduplication must compare incoming batch against labeling memory and remove matches | Done |
| DEDUP-4 | Deduplication must report stats: incoming count, dropped count, remaining count | Done |
| DEDUP-5 | Each LP pipeline (General, Czech) must have its own independent labeling memory | Done |
| DEDUP-6 | Deduped data must be saved to a `dropped_duplicates/` directory | Done |
| DEDUP-7 | After dedup, an analysis file must be automatically created from the remaining rows | Done |

---

## 6. Analysis / Labeling

This is the core human-in-the-loop step where operators review leads one at a time and make qualification decisions.

### 6.1 General Requirements

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| ANALYZE-1 | The system must present leads one record at a time with navigation (prev/next/skip) | Done |
| ANALYZE-2 | The system must track and display progress: count of YES, NO, unlabeled, total | Done |
| ANALYZE-3 | Navigation must auto-advance to the next **unlabeled** row (skipping already-labeled ones), with wraparound | Done |
| ANALYZE-4 | The system must support loading previously saved analysis files (resume interrupted sessions) | Done |
| ANALYZE-5 | All label changes must be persisted to the CSV immediately (no "save all" step) | Done |
| ANALYZE-6 | Label values must be normalized on every read/write: `y/yes/true/1` → `y`, `n/no/false/0` → `n` | Done |

### 6.2 Leadspicker Analysis

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| ANALYZE-LP-1 | Label actions: **YES** (relevant), **NO** (not relevant), **NO + Learning Data** (not relevant but useful for ML training) | Done |
| ANALYZE-LP-2 | YES must open the enrichment panel on the same row (stay, don't advance) | Done |
| ANALYZE-LP-3 | NO must auto-advance to next unlabeled row | Done |
| ANALYZE-LP-4 | NO + Learning Data must set both `relevant=n` and `learning_data=y`, then auto-advance | Done |
| ANALYZE-LP-5 | LinkedIn post text must be displayed with **keyword highlighting** (expansion-related terms wrapped in HTML tags) | Done |
| ANALYZE-LP-6 | Keywords to highlight: expand*, expansion*, scale*, grow/growth/grew/grown/growing, global*, worldwide, overseas, abroad, europe*, international*, cross-border, czech*, enter*, launch* | Done |
| ANALYZE-LP-7 | The **Czech pipeline** must additionally support a **CC (Compliance Checkpoint)** label that behaves like YES (opens enrichment) but stores `relevant="CC"` | Done |
| ANALYZE-LP-8 | In the Czech pipeline, CC must count as "done" for skip/navigation purposes (done_values = y, n, cc) | Done |
| ANALYZE-LP-9 | "Finish Labeling" must append all labeled rows (y/n/CC) to the pipeline's labeling memory, deduplicated by `lp_base_post_url` | Done |

### 6.3 Leadspicker Master Search (inline during analysis)

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| SEARCH-1 | The analysis UI must include an inline search bar to query the master log by company name | Done |
| SEARCH-2 | Search must return **exact matches** (normalized name equality) and **partial matches** (contains, case-insensitive), displayed separately | Done |
| SEARCH-3 | Results must show: company name (raw + normalized), domain, status, first/last seen dates, LP project ID, LP contact ID | Done |
| SEARCH-4 | Results must be capped at 10 per category (exact, partial) | Done |
| SEARCH-5 | The search query must be cleared automatically when the user performs any label or navigation action | Done |
| SEARCH-6 | The master log must be cached in memory for the duration of the session (module-level cache with optional force-reload) | Done |

### 6.4 Crunchbase Analysis

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| ANALYZE-CB-1 | Label actions: **YES** (relevant), **Eliminate** (sets Status="Eliminated"), **Un-eliminate** (reverts Status to "Longlist") | Done |
| ANALYZE-CB-2 | Editable fields per record: **Message fin** (final outreach message), **Main Contact** (LinkedIn URL), **Secondary Contact #1/#2/#3** (LinkedIn URLs) | Done |
| ANALYZE-CB-3 | "Save & Next" must set `Status="Quality B - Contacted"` and advance | Done |
| ANALYZE-CB-4 | "Save & Stay" must save fields without changing Status | Done |
| ANALYZE-CB-5 | "Finish Labeling" must save the current state and return to the Crunchbase menu | Done |

---

## 7. Enrichment

Enrichment is the manual step where the operator fills in or corrects contact details for leads marked as relevant.

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| ENRICH-1 | The enrichment panel must allow editing: **Full Name** (or separate First/Last), **LinkedIn URL** (person), **Relation to Company** (job title/role), **Company Name**, **Company Website**, **Company LinkedIn** | Done |
| ENRICH-2 | Full name must be split into first/last automatically using `_split_full_name()`: supports "First Last", "First Middle Last", and "Last, First" formats | Done |
| ENRICH-3 | When Company LinkedIn is provided, the cleaned version (trailing `/` stripped) must also be saved to `lp_company_linkedin_cleaned` | Done |
| ENRICH-4 | Two save modes: **Save & Next** (close panel, advance) and **Save & Stay** (keep panel open on same row) | Done |
| ENRICH-5 | Enrichment must be available both during **analysis** (after YES) and during **drafting** (inline edit) | Done |
| ENRICH-6 | In the Czech pipeline, enrichment form fields must **pre-fill with LP values** as fallback if the user doesn't modify them. The General pipeline does NOT do this (uses form values directly). | Done (inconsistency) |

---

## 8. Message Drafting

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| DRAFT-1 | Drafting must be initiated from an analyzed file, filtering to only `relevant="y"` rows | Done |
| DRAFT-2 | A `message_draft` column must be added if not present | Done |
| DRAFT-3 | The draft file must be saved as a new timestamped CSV in `drafted_messages/` | Done |
| DRAFT-4 | The drafting UI must show one record at a time with: author name + LinkedIn URL, company name + LinkedIn URL + website, relation/position, full LinkedIn post text, post URL, and editable message textarea | Done |
| DRAFT-5 | Draft text must be **auto-saved on every action** (including navigation) to prevent data loss | Done |
| DRAFT-6 | The UI must display contextual statements: whether the enriched contact **is the post author** (name match) and whether they're **from the company** or an outsider (company name match) | Done |
| DRAFT-7 | Individual rows must be removable from the drafting batch via a `remove_from_drafting` flag | Done |
| DRAFT-8 | A bulk edit view must allow selecting multiple rows for removal via checkboxes | Done |
| DRAFT-9 | "Finish Drafting" must drop all removed rows and save the cleaned file | Done |
| DRAFT-10 | The drafting route must detect which pipeline (General vs Czech) the file belongs to, and redirect accordingly after finishing | Done |

---

## 9. Push (Export to External Systems)

### 9.1 Push to Leadspicker API

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| PUSH-LP-1 | The system must push lead records to Leadspicker via `POST /api/persons` | Done |
| PUSH-LP-2 | Payload must include top-level fields: `first_name`, `last_name`, `linkedin`, `position`, `company_name`, `company_website`, `company_linkedin`, `email`, `country`, `salesnav` | Done |
| PUSH-LP-3 | Payload must include custom_fields: `base_post_url`, `Message - desc` (draft message), `linkedin_post`, `email_subject` | Done |
| PUSH-LP-4 | `project_id` and `data_source: "user_provided"` must be set on every payload | Done |
| PUSH-LP-5 | Field values must fall back from `global_*` to `lp_*` columns (e.g., `global_first_name` → `lp_lead_first_name`) | Done |
| PUSH-LP-6 | Push must collect per-row results: HTTP status, ok/fail, response text, errors | Done |
| PUSH-LP-7 | Push is currently **capped at 50 rows** as a WIP safety limit | Done (WIP) |

### 9.2 Push to Airtable

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| PUSH-AT-1 | For LP pipelines: the system must **create** new records in Airtable tables ("Leadspicker - general post" or "Leadspicker - czehcia post") using a configurable field mapping | Done |
| PUSH-AT-2 | The field mapping must support both direct column references and computed values (callables) | Done |
| PUSH-AT-3 | For CB pipeline: the system must **batch update** existing records in "Crunchbase Source" using the preserved `_id` | Done |
| PUSH-AT-4 | Airtable batch operations must use chunk size of 10 with `typecast=True` | Done |
| PUSH-AT-5 | CB batch update must drop problematic computed/formula columns before sending: `Half year reminder (suggested)`, `Message draft`, `CB financials link`, `CB people link`, `Contact enriched`, `CB news link`, `Tag`, `Tags`, `Reviewed by Roman`, `Number of Investors`, `relevant`, `learning_data` | Done |

### 9.3 Crunchbase → Leadspicker Cross-Push

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| PUSH-CB-LP-1 | CB records with `Status="Quality B - Contacted"` must be pushable to Leadspicker | Done |
| PUSH-CB-LP-2 | CB fields must be mapped to LP schema: `Message fin` → `message_draft`, `Name` → `global_company_name_raw`, `Company LinkedIn/Website`, contact URL from last non-empty of Main/Secondary Contact #1/#2/#3 → `global_linkedin_url` | Done |
| PUSH-CB-LP-3 | Rows with empty `message_draft` after mapping must be excluded from push | Done |

### 9.4 Post-Push Side Effects

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| POST-PUSH-1 | Successfully pushed rows must be appended to the pipeline's **labeling memory** (deduped by `lp_base_post_url`) | Done |
| POST-PUSH-2 | Successfully pushed rows must be appended to the **master log** with sequential IDs (`L{n:05d}`, `LP{n:05d}`) | Done |
| POST-PUSH-3 | Master log append must deduplicate against existing records by `global_leadspicker_contact_id` and normalized `lp_base_post_url` | Done |
| POST-PUSH-4 | Push timestamps (`global_pushed_leadspicker_at`, `global_pushed_airtable_at`) and `global_status="contacted"` must be written back to the drafted file | Done |
| POST-PUSH-5 | `message_draft` must be copied to `email_message_draft` in the master log entry | Done |
| POST-PUSH-6 | `global_source` must be set to `"LP"` if empty | Done |
| POST-PUSH-7 | `global_leadspicker_project` must be set to the target project ID | Done |

---

## 10. Master Log

The master log is the authoritative, cross-pipeline record of all leads that have been contacted.

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| MASTER-1 | The master log must be a single CSV file (`master_log.csv`) with a defined 73-column schema | Done |
| MASTER-2 | The schema must cover: IDs/routing (5), global identity/company (3), global identity/lead (4), workflow status (18), Crunchbase fields (28), Leadspicker fields (22+) | Done |
| MASTER-3 | New rows must receive sequential global IDs (format `L00001`) and source IDs (format `LP00001`) based on the current max in the file | Done |
| MASTER-4 | The master log must be queryable during analysis for inline company lookups (exact + partial name match) | Done |
| MASTER-5 | The master log must be written atomically (write to tmp, then rename) | Done |
| MASTER-6 | Extra columns not in the schema must be preserved at the end (no data loss on schema alignment) | Done |

---

## 11. Data Persistence & I/O

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| IO-1 | All data must be persisted as local CSV files with semicolon (`;`) delimiter and UTF-8 encoding | Done |
| IO-2 | All file writes must be atomic (write to `.tmp`, then `os.replace`) | Done |
| IO-3 | Empty strings must never be converted to NaN — `keep_default_na=False`, `na_filter=False` | Done |
| IO-4 | Malformed CSV lines must be skipped, not crash the application (`on_bad_lines="skip"`) | Done |
| IO-5 | Label normalization (y/n/CC) must be applied on every read and write of analysis/memory files | Done |
| IO-6 | The `relevant` and `learning_data` columns must be ensured to exist on every analysis file read | Done |
| IO-7 | All pipeline data directories must be auto-created on application startup | Done |
| IO-8 | Each pipeline stage must save its output to a dedicated directory: `fetches/`, `normalized_fetches/`, `dropped_duplicates/`, `analyzed/`, `drafted_messages/` | Done |

---

## 12. Navigation & UI Structure

### 12.1 Main Menu

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| NAV-1 | The home page must present the user with entry points to all lead source pipelines | Done — 4 logo buttons |
| NAV-2 | Leadspicker must have a sub-menu to choose between General and Czech pipelines | Done |

### 12.2 Pipeline Dashboards

Each pipeline has a dashboard page that serves as the control center for that pipeline's workflow.

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| NAV-3 | LP dashboard must show: import controls (API fetch / manual upload), project listing, dedup trigger, link to analysis, link to drafting, push controls, list of recent analysis files, list of drafted files | Done |
| NAV-4 | CB dashboard must show: Airtable fetch controls (with filter options), manual upload, staged file loading, link to analysis, push controls (Airtable and/or LP), list of recent analysis files | Done |
| NAV-5 | News dashboard must show: API fetch controls (with query/domains/dates/language), manual upload, result preview | Done |

### 12.3 Shared UI Patterns

| Req ID | Requirement | MVP Status |
|--------|-------------|------------|
| UI-1 | Analysis and drafting views must use a one-record-at-a-time card layout | Done |
| UI-2 | Progress indicator showing current position, total rows, and label counts | Done |
| UI-3 | Flash messages for success/error/warning/info feedback | Done |
| UI-4 | Data preview tables (first 20 rows) after import/fetch operations | Done |

---

## 13. External System Integrations

| System | Auth Method | Operations | Config |
|--------|-------------|------------|--------|
| **Leadspicker API** | API key (`X-API-Key` header) + dynamically scraped CSRF token | List projects, fetch people (paginated), create person | `LEADSPICKER_API_KEY` env var |
| **Airtable** | PAT (`Bearer` token) via pyairtable | Fetch records (with formula filters), batch update, batch create | `AIRTABLE_API_KEY` env var, hardcoded base ID `appSXOLAKJX3Vjo3n` |
| **NewsAPI.org** | API key (Authorization header) | Fetch everything (1 page, up to 100 results) | `NEWS_API_KEY` env var |

---

## 14. File Naming Conventions

| Pipeline | Stage | Pattern |
|----------|-------|---------|
| LP | Raw fetch | `LP_{YYYY-MM-DD_HHMM}_raw.csv` |
| LP | Normalized | `LP_{YYYY-MM-DD_HHMM}_normalized.csv` |
| LP | Deduped | `{original_stem}_deduped.csv` |
| LP | Analysis | `lp_labeling_analysis_{YYYYMMDD_HHMMSS}.csv` |
| LP | Drafted | `relevant_messages_drafted_{YYYYMMDD_HHMMSS}.csv` |
| CB | Airtable fetch | `CB_airtable_{YYYY-MM-DD_HHMMSS}.csv` |
| CB | Manual upload | `CB_{YYYY-MM-DD_HHMMSS}_raw.csv` |
| CB | Analysis | `cb_labeling_analysis_{YYYYMMDD_HHMMSS}.csv` |
| News | Raw fetch | `NEWS_{YYYY-MM-DD_HHMM}_raw.csv` |
| News | Normalized | `NEWS_{YYYY-MM-DD_HHMM}_normalized.csv` |

---

## 15. Known MVP Limitations (to address in production build)

| ID | Limitation | Impact |
|----|-----------|--------|
| LIM-1 | No authentication or user management | Anyone with network access can use the app |
| LIM-2 | Hardcoded Flask secret key | Sessions are insecure |
| LIM-3 | All data in local CSV files | No concurrent access, no backup strategy, no querying beyond pandas |
| LIM-4 | Single monolithic routes.py (2090 lines) | Hard to maintain and test |
| LIM-5 | No test suite | No automated verification of correctness |
| LIM-6 | News pipeline is fetch-only | No analysis, labeling, drafting, or push |
| LIM-7 | LP push capped at 50 rows | WIP safety measure needs a proper solution |
| LIM-8 | No pagination for news API | Only fetches 1 page (100 articles max) |
| LIM-9 | General vs Czech pipelines are 95% duplicated code | Maintenance burden, easy to diverge |
| LIM-10 | Airtable base/table IDs are hardcoded | Not configurable without code changes |
| LIM-11 | Czech enrichment has pre-fill fallback, General does not | Inconsistent behavior |
| LIM-12 | `lp_classifier` (AI classifier from LP) is extracted but lost in analysis file creation | Useful signal discarded |
| LIM-13 | Column name typo: `lp_comapny_linkedin_czech` | Would need migration if fixed |
| LIM-14 | Airtable table name typo: `"Leadspicker - czehcia post"` | Must match actual Airtable table name |
| LIM-15 | Module-level master log cache never invalidated | Stale data during long sessions |
| LIM-16 | `about.html` template exists but no route serves it | Dead template |
| LIM-17 | `done.html` template exists but no route uses it | Dead template |
| LIM-18 | Several defined functions are never called (`append_to_lp_memory_idempotent`, `get_table_ids`, `_cb_enrich_filename`) | Dead code |
| LIM-19 | `logic_master.py` is an empty file | Planned but never implemented |

---

## Appendix A: Complete Analysis Column Set (LP)

These are the columns present in LP analysis files (the subset operators see during labeling):

```
global_company_name_raw, global_first_name, global_last_name,
global_linkedin_url, global_relation_to_the_company,
global_leadspicker_project, global_leadspicker_contact_id,
global_email_subject, email_message_draft,
lp_lead_first_name, lp_lead_last_name, lp_company_linkedin,
lp_company_name, lp_company_website, lp_country, lp_created_at,
lp_email, lp_contacted_lead_linkedin, lp_base_post_url,
lp_summary, lp_relation_to_the_company, lp_lead_full_name,
lp_linkedin_post, lp_source_robot,
relevant, is_really_relevant(AI), learning_data
```

## Appendix B: Airtable Field Mapping (LP General Post)

| Airtable Field | Source Column / Computation |
|----------------|----------------------------|
| Company Name | `global_company_name_raw` |
| First Name | `global_first_name` |
| Full Name | computed: `first_name + " " + last_name` |
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

## Appendix C: Dropped Airtable Columns on CB Update

These columns are removed before batch-updating Crunchbase Source records (they're computed/formula fields in Airtable):

```
Half year reminder (suggested), Message draft, CB financials link,
CB people link, Contact enriched, CB news link, Tag, Tags,
Reviewed by Roman, Number of Investors, relevant, learning_data
```
