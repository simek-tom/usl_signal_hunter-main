from __future__ import annotations

from datetime import datetime
from pathlib import Path

from . import io_csv
import config
import pandas as pd

# Leadspicker API helpers (session, csrf, project listing / fetch)
from app.services import leadspicker_api


# ============================================================
# Master schema (authoritative column order) + align helper
# ============================================================

MASTER_COLUMNS = [
    "id","source_based_id","global_fingerprint","source_batch_id","global_source",
    "global_company_name_raw","global_company_name_norm","global_status","global_domain_norm",
    "global_first_name","global_last_name","global_linkedin_url","global_relation_to_the_company",
    "global_first_seen_at","global_last_seen_at","global_analyzed_at","global_message_drafted_at",
    "global_pushed_airtable_at","global_pushed_leadspicker_at","global_responded","global_responded_channel",
    "global_responded_at","global_response_sentiment","global_no_linkedin_messages","global_no_email_messages",
    "global_airtable_id","global_bounced","global_leadspicker_project","global_leadspicker_contact_id",
    "global_email_subject","email_message_draft",
    "cb_company_name","cb_status","cb_reviewed_by_roman","cb_message_ready","cb_created","cb_updated","cb_tag",
    "cb_crunchbase_profile","cb_financials_link","cb_people_link","cb_company_website","cb_series",
    "cb_last_funding_date","cb_industries","cb_number_of_employees","cb_number_of_investors",
    "cb_last_funding_amount_in_usd","cb_number_of_funding_rounds","cb_hq","cb_founded_on","cb_revenue_range",
    "cb_company_email","cb_main_contact","cb_secondary_contact_1","cb_secondary_contact_2","cb_secondary_contact_3",
    "cb_company_linkedin","cb_description","cb_investors",
    "lp_company_name","lp_status","lp_relation_to_the_company","lp_replied","lp_left_out",
    "lp_company_linkedin","lp_company_linkedin_cleaned","lp_company_linkedin_expand","lp_company_linkedin_expansion",
    "lp_company_linkedin_partnership","lp_company_linkedin_ceo","lp_comapny_linkedin_czech","lp_company_linkedin_slovak",
    "lp_slovak_linkedin_prague","lp_company_website","lp_country","lp_created_at","lp_lead_first_name","lp_lead_last_name",
    "lp_email","lp_contacted_lead_linkedin","lp_linkedin_post","lp_base_post_url","lp_summary","lp_source_robot"
]


def align_to_master_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all MASTER_COLUMNS exist and are ordered. Missing columns -> "".
    Extra columns (if any) are kept at the end to avoid data loss.
    """
    out = df.copy()
    for col in MASTER_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    extras = [c for c in out.columns if c not in MASTER_COLUMNS]
    return out[MASTER_COLUMNS + extras]

ANALYSIS_COLUMNS = [
    "global_company_name_raw",
    "global_first_name",
    "global_last_name",
    "global_linkedin_url",
    "global_relation_to_the_company",
    "global_leadspicker_project",
    "global_leadspicker_contact_id",
    "global_email_subject",
    "email_message_draft",
    "lp_lead_first_name",
    "lp_lead_last_name",
    "lp_company_linkedin",
    "lp_company_name",
    "lp_company_website",
    "lp_country",
    "lp_created_at",
    "lp_email",
    "lp_contacted_lead_linkedin",
    "lp_base_post_url",
    "lp_summary",
    "lp_relation_to_the_company",
    "lp_lead_full_name",
    "lp_linkedin_post",
    "lp_source_robot",
    "relevant",
    "is_really_relevant(AI)",
    "learning_data",
]

def build_analysis_from_stage(df_stage: pd.DataFrame, source_tag: str = "LP") -> pd.DataFrame:
    """
    Create the per-session analysis dataframe from the currently staged LP dataframe.
    - Pulls overlapping fields from 'master-like' staged df.
    - Adds analysis-only fields and sensible defaults.
    - Returns a frame with ANALYSIS_COLUMNS order (extras dropped).

    Parameters
    ----------
    df_stage : pd.DataFrame
        The normalized staged dataframe (what you’re previewing/deduping).
    source_tag : str
        A label for the 'lp_source_robot' column (e.g., 'LP', 'LP_API', 'Manual').

    Returns
    -------
    pd.DataFrame
        Dataframe aligned to ANALYSIS_COLUMNS ready to save to lp_labeling_analysis{timestamp}.csv
    """
    def pick(*candidates: str) -> pd.Series:
        for c in candidates:
            if c in df_stage.columns:
                return df_stage[c].astype(str)
        # empty if nothing
        return pd.Series([""] * len(df_stage), index=df_stage.index, dtype="string")

    # Company/lead “global_*” side
    out = pd.DataFrame(index=df_stage.index)
    out["global_company_name_raw"]       = pick("global_company_name_raw", "lp_company_name")
    out["global_first_name"]             = pick("global_first_name", "lp_lead_first_name")
    out["global_last_name"]              = pick("global_last_name",  "lp_lead_last_name")
    out["global_linkedin_url"]           = pick("global_linkedin_url", "lp_contacted_lead_linkedin")
    out["global_relation_to_the_company"]= pick("global_relation_to_the_company", "lp_relation_to_the_company")
    out["global_leadspicker_project"]    = pick("global_leadspicker_project")
    out["global_leadspicker_contact_id"] = pick("global_leadspicker_contact_id", "id")
    out["global_email_subject"]          = pick("global_email_subject")
    out["email_message_draft"]           = pick("email_message_draft")

    # LP-side fields
    # Prefer cleaned LinkedIn if available; fallback to raw
    lp_li_clean = pick("lp_company_linkedin_cleaned")
    lp_li_raw   = pick("lp_company_linkedin")
    out["lp_company_linkedin"]           = lp_li_clean.where(lp_li_clean.ne(""), lp_li_raw)

    out["lp_company_name"]               = pick("lp_company_name", "global_company_name_raw")
    out["lp_company_website"]            = pick("lp_company_website")
    out["lp_country"]                    = pick("lp_country")
    out["lp_created_at"]                 = pick("lp_created_at")
    out["lp_email"]                      = pick("lp_email")
    out["lp_contacted_lead_linkedin"]    = pick("lp_contacted_lead_linkedin")
    out["lp_base_post_url"]              = pick("lp_base_post_url")
    out["lp_summary"]                    = pick("lp_summary")
    out["lp_relation_to_the_company"]    = pick("lp_relation_to_the_company")
    out["lp_lead_first_name"]            = pick("lp_lead_first_name", "global_first_name")
    out["lp_lead_last_name"]             = pick("lp_lead_last_name",  "global_last_name")
    out["lp_linkedin_post"]              = pick("lp_linkedin_post")

    # Convenience: full name
    out["lp_lead_full_name"] = (
        out["lp_lead_first_name"].fillna("").str.strip() + " " +
        out["lp_lead_last_name"].fillna("").str.strip()
    ).str.strip()

    # Analysis-only fields
    # If lp_source_robot already exists (e.g. from contact_data.source_robot.value),
    # keep it; otherwise fall back to the provided source_tag.
    existing_robot = pick("lp_source_robot")
    out["lp_source_robot"] = existing_robot.where(existing_robot.ne(""), source_tag)
    out["relevant"]                     = ""      # set to "yes"/"no" later during labeling
    out["is_really_relevant(AI)"]       = ""      # future AI review / QA
    out["learning_data"]                = ""      # set when "NO — but learning data YES"

    # Ensure exact column order (and only these columns)
    out = out.reindex(columns=ANALYSIS_COLUMNS)
    return out

# ============================================================
# General helpers
# ============================================================

def _series_of_len(df: pd.DataFrame, fill: str = "") -> pd.Series:
    """Return an empty/fill string Series with the same index/length as df."""
    return pd.Series([fill] * len(df), index=df.index, dtype="string")


def _pick(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    """
    Return the first existing column among candidates (exact match first),
    then try case-insensitive match. If none found, return empty series.
    """
    # exact match
    for c in candidates:
        if c in df.columns:
            return df[c].astype(str)
    # case-insensitive fallback
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        lc = c.lower()
        if lc in lower_map:
            return df[lower_map[lc]].astype(str)
    return _series_of_len(df)


def _norm_url(s: pd.Series) -> pd.Series:
    """
    Normalize URLs for matching: lowercase, trim, remove spaces, strip protocol/www, no trailing slash.
    """
    s = s.astype(str).str.strip().str.lower()
    s = s.str.replace(r"\s", "", regex=True)            # remove spaces
    s = s.str.replace(r"^https?://", "", regex=True)    # drop protocol
    s = s.str.replace(r"^www\.", "", regex=True)        # drop www.
    s = s.str.rstrip("/")                               # drop trailing slash
    return s


# ============================================================
# Leadspicker fetch (API) + manual load
# ============================================================

def fetch_api(project_ids: list[str], since_date: str) -> pd.DataFrame:
    """
    Mirror the manual flow:
      1) session, csrf = get_session_and_csrf()
      2) for each project_id: process_project_info(session, csrf, project_id)
      3) concat frames → return raw df
    'since_date' is reserved for future filtering if needed.
    """
    res = leadspicker_api.get_session_and_csrf()
    if isinstance(res, tuple) and len(res) == 3:
        session, csrf_token, debug = res
    elif isinstance(res, tuple) and len(res) == 2:
        session, csrf_token = res
        debug = None
    else:
        session = csrf_token = None
        debug = "Unexpected return from get_session_and_csrf()"

    if not session or not csrf_token:
        return pd.DataFrame()

    frames = []
    for pid in project_ids:
        df_project = leadspicker_api.process_project_info(session, csrf_token, pid)
        if df_project is not None and not df_project.empty:
            frames.append(df_project)

    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()


def load_manual_csv(file_path: Path) -> pd.DataFrame:
    """
    Load a manually uploaded CSV (UTF-8, ; delimiter expected).
    Return a *raw* df that normalize_to_master_like() can handle.
    """
    return pd.read_csv(file_path, sep=";", dtype=str, keep_default_na=False).fillna("")


# ============================================================
# LP raw → Master-like mapping
# ============================================================

def _trim_at_5th_slash_series(s: pd.Series) -> pd.Series:
    """
    If a URL has 5 or more '/' characters, cut the string right BEFORE the 5th slash.
    If it has only 4 or fewer '/', return as-is.
    """
    def trim_one(u: str) -> str:
        if not isinstance(u, str):
            u = "" if pd.isna(u) else str(u)
        count = 0
        for i, ch in enumerate(u):
            if ch == '/':
                count += 1
                if count == 5:
                    return u[:i]  # cut off everything from the 5th slash onward
        return u
    return s.astype(str).apply(trim_one)

def normalize_to_master_like(df_raw: pd.DataFrame, source_batch_id: str) -> pd.DataFrame:
    """
    Map LP API dataframe → your master schema. Populates what we can from LP:
      - global_* identity fields (company name/domain + lead info)
      - global_leadspicker_contact_id
      - lp_* fields (including the 'people/?keywords=' links)
    Everything else stays "" as a placeholder.
    """
    out = pd.DataFrame(index=df_raw.index)

    # LP raw columns (based on your provided mapping)
    lp_contact_id   = _pick(df_raw, ["id"])
    lp_company_name = _pick(df_raw, ["contact_data.company_name.value"])
    lp_company_ln   = _pick(df_raw, ["contact_data.company_linkedin.value"])
    lp_company_web  = _pick(df_raw, ["contact_data.company_website.value"])
    lp_country      = _pick(df_raw, ["contact_data.country.value"])
    lp_created_at   = _pick(df_raw, ["created"])
    lp_first        = _pick(df_raw, ["contact_data.first_name.value"])
    lp_last         = _pick(df_raw, ["contact_data.last_name.value"])
    lp_email        = _pick(df_raw, ["contact_data.email.value"])
    lp_lead_li      = _pick(df_raw, ["contact_data.linkedin.value"])
    lp_position     = _pick(df_raw, ["contact_data.position.value"])
    lp_source_robot = _pick(df_raw, ["contact_data.source_robot.value"])
    lp_left_out     = _pick(df_raw, ["is_left_out"])
    lp_post         = _pick(df_raw, ["contact_data.post_content.value"])
    lp_post_url     = _pick(df_raw, ["contact_data.linkedin_post_url.value"])

    # IDs / routing
    out["id"]               = ""                    # assign when merging to master
    out["source_based_id"]  = ""                    # assign per-source on insert
    out["source_batch_id"]  = source_batch_id
    out["global_source"]    = "LP"

    # Global identity (company)
    out["global_company_name_raw"]  = lp_company_name
    out["global_company_name_norm"] = lp_company_name.str.lower().str.strip()

    web = lp_company_web.str.lower().str.strip()
    dom = (web
           .str.replace(r"^https?://", "", regex=True)
           .str.replace(r"^www\.", "", regex=True)
           .str.split("/", n=1).str[0]
           .str.rstrip("/"))
    out["global_domain_norm"] = dom

    # Global identity (lead/contact)
    out["global_first_name"]               = lp_first
    out["global_last_name"]                = lp_last
    out["global_linkedin_url"]             = lp_lead_li
    out["global_relation_to_the_company"]  = lp_position

    # Fingerprint (simple, deterministic)
    out["global_fingerprint"] = (out["global_company_name_norm"].fillna("") + "|" +
                                 out["global_domain_norm"].fillna(""))

    # Global workflow placeholders
    today = datetime.now().strftime("%Y-%m-%d")
    out["global_status"]                = "new"
    out["global_first_seen_at"]         = today
    out["global_last_seen_at"]          = today
    out["global_analyzed_at"]           = ""
    out["global_message_drafted_at"]    = ""
    out["global_pushed_airtable_at"]    = ""
    out["global_pushed_leadspicker_at"] = ""
    out["global_responded"]             = ""
    out["global_responded_channel"]     = ""
    out["global_responded_at"]          = ""
    out["global_response_sentiment"]    = ""
    out["global_no_linkedin_messages"]  = ""
    out["global_no_email_messages"]     = ""
    out["global_airtable_id"]           = ""
    out["global_bounced"]               = ""
    out["global_leadspicker_project"]   = ""  # optional, set later if you track LP project id
    out["global_leadspicker_contact_id"]= lp_contact_id
    out["global_email_subject"]         = ""  # to be filled during analysis
    out["email_message_draft"]          = ""  # to be filled during analysis

    # Crunchbase fields (empty for LP)
    cb_cols = [
        "cb_company_name","cb_status","cb_reviewed_by_roman","cb_message_ready","cb_created","cb_updated","cb_tag",
        "cb_crunchbase_profile","cb_financials_link","cb_people_link","cb_company_website","cb_series",
        "cb_last_funding_date","cb_industries","cb_number_of_employees","cb_number_of_investors",
        "cb_last_funding_amount_in_usd","cb_number_of_funding_rounds","cb_hq","cb_founded_on","cb_revenue_range",
        "cb_company_email","cb_main_contact","cb_secondary_contact_1","cb_secondary_contact_2","cb_secondary_contact_3",
        "cb_company_linkedin","cb_description","cb_investors"
    ]
    for c in cb_cols:
        out[c] = ""

    # LP-specific fields
    out["lp_company_name"]               = lp_company_name
    out["lp_status"]                     = ""
    out["lp_relation_to_the_company"]    = lp_position
    out["lp_replied"]                    = ""
    out["lp_left_out"]                   = lp_left_out
    out["lp_company_linkedin"]           = lp_company_ln
    out["lp_company_linkedin_cleaned"] = _trim_at_5th_slash_series(lp_company_ln).str.rstrip("/")

    # Only build keyword links if we actually have a base URL
    def _build_people_links(s: pd.Series, keyword: str) -> pd.Series:
        return s.apply(lambda u: f"{u}/people/?keywords={keyword}" if u else "")

    out["lp_company_linkedin_expand"]      = _build_people_links(out["lp_company_linkedin_cleaned"], "expand")
    out["lp_company_linkedin_expansion"]   = _build_people_links(out["lp_company_linkedin_cleaned"], "expansion")
    out["lp_company_linkedin_partnership"] = _build_people_links(out["lp_company_linkedin_cleaned"], "partnership")
    out["lp_company_linkedin_ceo"]         = _build_people_links(out["lp_company_linkedin_cleaned"], "ceo")
    out["lp_comapny_linkedin_czech"]       = _build_people_links(out["lp_company_linkedin_cleaned"], "czech")
    out["lp_company_linkedin_slovak"]      = _build_people_links(out["lp_company_linkedin_cleaned"], "slovak")
    out["lp_slovak_linkedin_prague"]       = _build_people_links(out["lp_company_linkedin_cleaned"], "prague")

    out["lp_company_website"]            = lp_company_web
    out["lp_country"]                    = lp_country
    out["lp_created_at"]                 = lp_created_at
    out["lp_lead_first_name"]            = lp_first
    out["lp_lead_last_name"]             = lp_last
    out["lp_email"]                      = lp_email
    out["lp_contacted_lead_linkedin"]    = lp_lead_li
    out["lp_linkedin_post"]              = lp_post
    out["lp_base_post_url"]              = lp_post_url
    out["lp_summary"]                    = _pick(df_raw, ["contact_data.summary.value"])
    out["lp_classifier"]                 = _pick(df_raw, ["contact_data.ai_classifier.value"])
    out["lp_source_robot"]               = lp_source_robot

    # Align to master schema (order + ensure all columns)
    out = align_to_master_schema(out)
    return out


# ============================================================
# Dedupe vs LP labeling memory (by lp_base_post_url)
# ============================================================

def drop_dupes_against_lp_memory(df_new: pd.DataFrame, df_mem: pd.DataFrame):
    """
    Keep only rows in df_new whose lp_base_post_url is NOT present in df_mem.
    Matching uses a normalized URL.
    Returns: (df_filtered, stats)
    """
    if "lp_base_post_url" not in df_new.columns:
        df_new["lp_base_post_url"] = ""
    if "lp_base_post_url" not in df_mem.columns:
        df_mem["lp_base_post_url"] = ""

    new_urls    = _norm_url(df_new["lp_base_post_url"])
    memory_urls = set(_norm_url(df_mem["lp_base_post_url"]))
    is_dup      = (new_urls != "") & new_urls.isin(memory_urls)

    dropped = int(is_dup.sum())
    df_out  = df_new.loc[~is_dup].copy()

    stats = {"incoming": int(len(df_new)), "dropped": dropped, "remaining": int(len(df_out))}
    return df_out, stats

def _lp_analysis_filename(ts: str | None = None, analyses_dir: Path | None = None) -> Path:
    """
    Build a timestamped analysis filename under the given analyses_dir
    (defaults to config.LP_ANALYSES_DIR).
    Example: data/leadspicker/analyzed/lp_labeling_analysis_20250901_141530.csv
    """
    ts = ts or datetime.now().strftime("%Y%m%d_%H%M%S")
    base = analyses_dir or config.LP_ANALYSES_DIR
    return base / f"lp_labeling_analysis_{ts}.csv"

def save_lp_analysis_from_df(df: pd.DataFrame, ts: str | None = None, analyses_dir: Path | None = None) -> Path:
    """
    Ensure analysis columns exist/normalized and write the analysis cache CSV atomically.
    """
    df = io_csv.ensure_analysis_has_columns(df.copy())
    path = _lp_analysis_filename(ts, analyses_dir=analyses_dir)
    io_csv.write_lp_analysis(path, df)
    return path

def save_lp_analysis_from_csv(staged_csv_path: str | Path, ts: str | None = None) -> Path:
    df = io_csv.read_csv(Path(staged_csv_path))
    df = io_csv.ensure_analysis_has_columns(df)
    return save_lp_analysis_from_df(df, ts=ts)

def dedupe_and_create_analysis(df: pd.DataFrame, ts: str | None = None) -> tuple[pd.DataFrame, dict, Path]:
    """
    Convenience: perform dedupe against lp memory, then create an analysis file
    from the deduped rows. Returns (deduped_df, stats, analysis_path).
    """
    # Read current LP labeling memory and pass it in
    mem_df = io_csv.read_lp_labeling_memory()
    deduped_df, stats = drop_dupes_against_lp_memory(df, mem_df)
    analysis_path = save_lp_analysis_from_df(deduped_df, ts=ts)
    return deduped_df, stats, analysis_path


# ============================================================
# Master log append helpers (LP drafted -> master)
# ============================================================

def _next_sequence_value(series: pd.Series, prefix: str) -> int:
    """
    Return the next integer sequence for IDs with a fixed prefix, e.g. 'L00010'.
    If no value is present, returns 0 so callers can start at prefix + 00000.
    """
    max_val = -1
    if series is None or series.empty:
        return 0
    for raw in series.astype(str):
        s = raw.strip()
        if not s or not s.startswith(prefix):
            continue
        suffix = s[len(prefix) :]
        if suffix.isdigit():
            max_val = max(max_val, int(suffix))
    return (max_val + 1) if max_val >= 0 else 0


def append_lp_rows_to_master(df_rows: pd.DataFrame, project_id: int | str) -> dict:
    """
    Take drafted LP rows (with message drafts) and append them to the master log.
    - Ensures master schema alignment.
    - Assigns sequential id/source_based_id.
    - Skips rows already present (contact_id or base_post_url duplicate).
    Returns stats dict with counts.
    """
    if df_rows is None or df_rows.empty:
        return {"total": 0, "appended": 0, "skipped_duplicates": 0}

    df_new = align_to_master_schema(df_rows.copy())
    project_id_str = str(project_id).strip()

    # Prepare enriched columns
    today = datetime.now().strftime("%Y-%m-%d")
    if "global_source" not in df_new.columns:
        df_new["global_source"] = ""
    df_new["global_source"] = df_new["global_source"].astype(str).str.strip()
    df_new.loc[df_new["global_source"] == "", "global_source"] = "LP"
    df_new["global_leadspicker_project"] = project_id_str

    for col in ["global_status", "global_message_drafted_at", "global_pushed_leadspicker_at", "global_last_seen_at"]:
        if col not in df_new.columns:
            df_new[col] = ""

    df_new["global_status"] = "contacted"
    df_new["global_message_drafted_at"] = today
    df_new["global_pushed_leadspicker_at"] = today
    df_new["global_last_seen_at"] = df_new["global_last_seen_at"].where(
        df_new["global_last_seen_at"].astype(str).str.strip() != "", today
    )

    # Copy drafted message text into master column if present
    if "message_draft" in df_new.columns:
        if "email_message_draft" not in df_new.columns:
            df_new["email_message_draft"] = ""
        draft_series = df_new["message_draft"].astype(str)
        mask = df_new["email_message_draft"].astype(str).str.strip() == ""
        df_new.loc[mask, "email_message_draft"] = draft_series[mask]

    # Ensure fingerprint exists for search
    if "global_fingerprint" in df_new.columns:
        fp_mask = df_new["global_fingerprint"].astype(str).str.strip() == ""
        df_new.loc[fp_mask, "global_fingerprint"] = (
            df_new.loc[fp_mask, "global_company_name_norm"].astype(str).str.strip()
            + "|"
            + df_new.loc[fp_mask, "global_domain_norm"].astype(str).str.strip()
        )

    # Load master and dedupe
    master_df = io_csv.read_csv(config.MASTER_FILE)
    if master_df is None or master_df.empty:
        master_df = pd.DataFrame(columns=MASTER_COLUMNS)
    else:
        master_df = master_df.reindex(columns=MASTER_COLUMNS, fill_value="")

    dup_mask = pd.Series(False, index=df_new.index)

    if "global_leadspicker_contact_id" in df_new.columns and "global_leadspicker_contact_id" in master_df.columns:
        existing_contacts = set(master_df["global_leadspicker_contact_id"].astype(str).str.strip())
        contacts = df_new["global_leadspicker_contact_id"].astype(str).str.strip()
        dup_mask |= (contacts != "") & contacts.isin(existing_contacts)

    if "lp_base_post_url" in df_new.columns and "lp_base_post_url" in master_df.columns:
        existing_posts = set(_norm_url(master_df["lp_base_post_url"]))
        new_posts = _norm_url(df_new["lp_base_post_url"])
        dup_mask |= (new_posts != "") & new_posts.isin(existing_posts)

    df_filtered = df_new.loc[~dup_mask].copy()
    skipped = int(dup_mask.sum())

    if df_filtered.empty:
        return {"total": len(df_new), "appended": 0, "skipped_duplicates": skipped}

    next_id_base = _next_sequence_value(master_df["id"], "L")
    next_source_base = _next_sequence_value(master_df["source_based_id"], "LP")

    id_values = []
    source_values = []
    for offset in range(len(df_filtered)):
        id_values.append(f"L{next_id_base + offset:05d}")
        source_values.append(f"LP{next_source_base + offset:05d}")

    df_filtered.loc[:, "id"] = id_values
    df_filtered.loc[:, "source_based_id"] = source_values

    df_out = df_filtered[MASTER_COLUMNS].copy()
    master_updated = pd.concat([master_df, df_out], ignore_index=True)

    io_csv.write_csv_atomic(config.MASTER_FILE, master_updated)

    return {
        "total": len(df_new),
        "appended": len(df_filtered),
        "skipped_duplicates": skipped,
        "assigned_ids": id_values,
        "assigned_source_ids": source_values,
    }
