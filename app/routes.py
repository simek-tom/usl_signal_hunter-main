# app/routes.py
from app import app
from flask import render_template, request, redirect, url_for, session, flash
from flask.typing import ResponseReturnValue
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import config
from pyairtable import Api
import pyairtable
from app.services import io_csv, leadspicker_api, airtable_api, news_ingest, news_api
from .services import leadspicker_ingest
import re
from markupsafe import Markup, escape
from typing import Optional


from config import (
    LP_FETCH_DIR,
    LP_NORMALIZED_DIR,
    LP_DROPPED_DIR,
    LP_ANALYSES_DIR,
    MASTER_FILE,                  # kept for future steps
)

from app.services.leadspicker_ingest import (
    fetch_api,
    normalize_to_master_like,
    load_manual_csv,
    drop_dupes_against_lp_memory,
    align_to_master_schema,
    build_analysis_from_stage,    # builds the per-session analysis CSV
)

# ---------- Helpers for analysis UI ----------
KEYWORD_RE = re.compile(
    r"\b("
    r"expand(?:s|ed|ing)?|"
    r"expansion(?:s)?|"
    r"scale(?:s|d|ing)?|"
    r"grow|grows|grew|grown|growing|growth|"
    r"global(?:ly)?|worldwide|overseas|abroad|"
    r"europe(?:an)?|"
    r"international(?:ly)?|cross[- ]?border"
    r")\b",
    flags=re.IGNORECASE
)

def highlight_keywords(text: str) -> Markup:
    if not text:
        return Markup("")
    safe = escape(text)
    return Markup(KEYWORD_RE.sub(lambda m: f"<mark>{m.group(0)}</mark>", safe))


def _is_under(base: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(base.resolve())
        return True
    except Exception:
        return False

def _first_unlabeled_index(
    df: pd.DataFrame,
    start: int = 0,
    done_values: Optional[set[str]] = None,
) -> int | None:
    if df is None or df.empty:
        return None
    rel = df.get("relevant")
    if rel is None:
        return 0
    n = len(df)
    # Default: treat only 'y' and 'n' as completed labels
    done_values = done_values or {"y", "n"}
    # search forward from start, then wrap to 0
    for i in list(range(start, n)) + list(range(0, start)):
        v = str(rel.iat[i]).strip().lower()
        if v not in done_values:
            return i
    return None

def _label_counts(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {"y": 0, "n": 0, "unlabeled": 0, "total": 0}
    s = df.get("relevant")
    if s is None:
        return {"y": 0, "n": 0, "unlabeled": len(df), "total": len(df)}
    y = (s.str.lower() == "y").sum()
    n = (s.str.lower() == "n").sum()
    total = len(df)
    return {"y": int(y), "n": int(n), "unlabeled": int(total - y - n), "total": int(total)}


# Highlight keywords in LP post text
@app.template_filter("highlight_lp_keywords")
def highlight_lp_keywords(text: str) -> str:
    if not text:
        return ""
    pattern = re.compile(
        r"\b("
        r"expand(?:s|ed|ing)?|"
        r"expansion(?:s)?|"
        r"scale(?:s|d|ing)?|"
        r"grow|grows|grew|grown|growing|growth|"
        r"global(?:ly)?|worldwide|overseas|abroad|"
        r"europe(?:an)?|"
        r"international(?:ly)?|cross[- ]?border|"
        r"czech(?:ia)?|czech republic|czech-republic|czechrepublic|"
        r"enter(?:s|ed|ing)?|"
        r"launch(?:es|ed|ing)?"
        r")\b",
        re.IGNORECASE,
    )
    return pattern.sub(lambda m: f'<span class="hl">{m.group(0)}</span>', str(text))


def _list_drafted_files_for_dir(dir_path: Path) -> list[Path]:
    files = [p for p in dir_path.glob("*.csv") if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files

def _list_drafted_files() -> list[Path]:
    return _list_drafted_files_for_dir(config.LP_DRAFTED_DIR)

def _list_cb_stage_files(limit: int = 50) -> list[Path]:
    """
    List Crunchbase fetch CSVs newest-first.
    """
    base = getattr(config, "CB_FETCH_DIR", None)
    if not base:
        return []
    files = [p for p in Path(base).glob("*.csv") if p.is_file()]
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:limit]

def _new_drafted_filename_for_dir(dir_path: Path) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return dir_path / f"relevant_messages_drafted_{ts}.csv"

def _new_drafted_filename() -> Path:
    return _new_drafted_filename_for_dir(config.LP_DRAFTED_DIR)

def _cb_analysis_filename(ts: str | None = None) -> Path:
    ts = ts or datetime.now().strftime("%Y%m%d_%H%M%S")
    return config.CB_ANALYSES_DIR / f"cb_labeling_analysis_{ts}.csv"

def _cb_enrich_filename(ts: str | None = None) -> Path:
    ts = ts or datetime.now().strftime("%Y%m%d_%H%M%S")
    return config.CB_ENRICH_DIR / f"cb_relevant_{ts}.csv"

def _split_full_name_cb(name: str) -> tuple[str, str]:
    # reuse main splitter
    return _split_full_name(name)

def _cb_to_lp_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map Crunchbase analysis columns into the minimal schema expected by
    leadspicker_api.push_drafted_dataframe. Mapping (LP field -> CB column):
      - contact_data.linkedin.value     -> last filled of [Main Contact, Secondary Contact #1, #2, #3]
      - contact_data.company_name.value -> Name
      - contact_data.company_website.value -> Company Website
      - contact_data.company_linkedin.value -> Company LinkedIn
      - contact_data.message_draft.value -> Message fin
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "global_first_name", "global_last_name",
            "global_company_name_raw", "global_company_linkedin",
            "global_domain_norm", "message_draft", "global_linkedin_url",
        ])

    out = pd.DataFrame()
    out["message_draft"] = df.get("Message fin", df.get("Message draft", "")).astype(str)
    out["global_company_name_raw"] = df.get("Name", "").astype(str)
    out["global_company_linkedin"] = df.get("Company LinkedIn", "").astype(str)
    out["global_domain_norm"] = df.get("Company Website", "").astype(str)

    def _pick_contact(row: pd.Series) -> str:
        # Prefer the last non-empty among Main Contact, Secondary #1..#3
        candidates = [
            row.get("Main Contact", ""),
            row.get("Secondary Contact #1", ""),
            row.get("Secondary Contact #2", ""),
            row.get("Secondary Contact #3", ""),
        ]
        chosen = ""
        for val in candidates:
            sval = "" if val is None else str(val).strip()
            if sval:
                chosen = sval  # overwrite so we end up with the last non-empty
        return chosen

    linkedin_list: list[str] = []
    for _, row in df.iterrows():
        contact_val = _pick_contact(row)
        linkedin_list.append(contact_val)
        first, last = _split_full_name_cb(str(contact_val))

    out["global_linkedin_url"] = linkedin_list
    return out

# ---- Master lookup cache + normalizer ----
_MASTER_DF: pd.DataFrame | None = None

def _norm_company_name(s: str) -> str:
    # lowercase, trim, collapse whitespace
    return " ".join((s or "").lower().split())

def _get_master_df(force: bool = False) -> pd.DataFrame:
    global _MASTER_DF
    if force or _MASTER_DF is None:
        try:
            df = pd.read_csv(config.MASTER_FILE, sep=";", dtype=str, keep_default_na=False).fillna("")
        except Exception:
            df = pd.DataFrame()
        if not df.empty:
            if "global_company_name_norm" in df.columns:
                df["global_company_name_norm"] = df["global_company_name_norm"].astype(str).map(_norm_company_name)
            elif "global_company_name_raw" in df.columns:
                df["global_company_name_norm"] = df["global_company_name_raw"].astype(str).map(_norm_company_name)
            else:
                df["global_company_name_norm"] = ""
        _MASTER_DF = df
    return _MASTER_DF

# Clear the Analyze search bar state
def _reset_master_search() -> None:
    # safely drop the stored query from session
    session.pop("lp_master_query", None)

def _reset_master_search_cz() -> None:
    session.pop("lp_cz_master_query", None)

# ---------- Routes ----------
@app.route("/")
def index() -> ResponseReturnValue:
    return render_template("index.html")


@app.route("/leadspicker/menu")
def leadspicker_menu() -> ResponseReturnValue:
    return render_template("leadspicker/menu.html")


@app.route("/leadspicker", methods=["GET", "POST"])
def leadspicker() -> ResponseReturnValue:
    columns, rows, message = [], [], ""
    projects = []
    recent_analysis_files = io_csv.list_lp_analysis_files(limit=20)
    current_analysis_path = session.get("lp_analysis_path")
    current_analysis_name = Path(current_analysis_path).name if current_analysis_path else None
    drafted_files = _list_drafted_files()
    drafted_names = [p.name for p in drafted_files]
    latest_drafted = drafted_names[0] if drafted_names else ""

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        ts = datetime.now().strftime("%Y-%m-%d_%H%M")
        source_batch_id = f"LP-{datetime.now().strftime('%d-%m-%Y')}"
        staged_norm = config.LP_NORMALIZED_DIR / f"LP_{ts}_normalized.csv"

        # ---------- A) LIST PROJECTS ----------
        if action == "list_projects":
            session_obj, csrf, debug = leadspicker_api.get_session_and_csrf()
            if not session_obj or not csrf:
                message = f"Failed to start session or retrieve CSRF. {debug}"
            else:
                projects = leadspicker_api.get_project_ids(session_obj, csrf)
                message = f"Found {len(projects)} projects." if projects else "No projects returned."

        # ---------- B) FETCH VIA API ----------
        elif action == "api_fetch":
            raw_ids = request.form.get("project_ids", "11870")
            project_ids = [p.strip() for p in raw_ids.split(",") if p.strip()]
            df_raw = fetch_api(project_ids=project_ids, since_date="")
            if df_raw is None or df_raw.empty:
                message = "No records returned from API for given project ID(s)."
            else:
                raw_path = config.LP_FETCH_DIR / f"LP_{ts}_raw.csv"
                df_raw.to_csv(raw_path, sep=";", index=False, encoding="utf-8")
                df_norm = normalize_to_master_like(df_raw, source_batch_id)
                df_norm = align_to_master_schema(df_norm)
                df_norm.to_csv(staged_norm, sep=";", index=False, encoding="utf-8")
                session["lp_current_stage"] = str(staged_norm)
                session["lp_ready_to_analyze"] = False
                session.pop("lp_analyze_idx", None)
                session.pop("lp_analysis_file", None)

                columns, rows = df_norm.columns.tolist(), df_norm.head(20).to_dict("records")
                message = f"Fetched {len(df_norm)} records from project(s): {', '.join(project_ids)}. Ready to drop duplicates."

        # ---------- C) MANUAL UPLOAD ----------
        elif action == "manual":
            file = request.files.get("file")
            if file and file.filename:
                raw_path = config.LP_FETCH_DIR / f"LP_{ts}_raw.csv"
                file.save(raw_path)
                df_raw = load_manual_csv(raw_path)
                if df_raw is None or df_raw.empty:
                    message = "Uploaded file is empty or unreadable."
                else:
                    df_norm = normalize_to_master_like(df_raw, source_batch_id)
                    df_norm = align_to_master_schema(df_norm)
                    df_norm.to_csv(staged_norm, sep=";", index=False, encoding="utf-8")
                    session["lp_current_stage"] = str(staged_norm)
                    session["lp_ready_to_analyze"] = False
                    session.pop("lp_analyze_idx", None)
                    session.pop("lp_analysis_file", None)

                    columns, rows = df_norm.columns.tolist(), df_norm.head(20).to_dict("records")
                    message = f"Loaded {len(df_norm)} records from upload. Ready to drop duplicates."
            else:
                message = "Please choose a CSV file to upload."

        # ---------- D) DROP DUPLICATES vs LP LABELING MEMORY ----------
        elif action == "drop_dupes":
            staged_path = session.get("lp_current_stage")
            if not staged_path or not Path(staged_path).exists():
                message = "No staged data found. Please import first."
            else:
                df_stage = pd.read_csv(staged_path, sep=";", dtype=str, keep_default_na=False).fillna("")
                df_mem = io_csv.read_lp_labeling_memory()

                df_dedup, stats = drop_dupes_against_lp_memory(df_stage, df_mem)

                staged_dedup = config.LP_DROPPED_DIR / f"{Path(staged_path).stem}_deduped.csv"
                df_dedup.to_csv(staged_dedup, sep=";", index=False, encoding="utf-8")
                session["lp_current_stage"] = str(staged_dedup)
                session["lp_ready_to_analyze"] = True
                session["lp_analyze_idx"] = 0
                session.pop("lp_analysis_file", None)

                # After dedupe, immediately create an analysis cache file and store it in session.
                try:
                    analysis_path = leadspicker_ingest.save_lp_analysis_from_df(df_dedup)
                    session["lp_analysis_path"] = str(analysis_path)
                    session.pop("lp_analysis_row_idx", None)  # reset pointer
                    flash(f"Analysis file created: {Path(analysis_path).name}", "info")
                except Exception as e:
                    flash(f"Failed to create analysis file: {e}", "error")

                columns = df_dedup.columns.tolist()
                rows    = df_dedup.head(20).to_dict("records")
                message = (
                    f"Dropped {stats['dropped']} / {stats['incoming']} posts already in labeling memory. "
                    f"Remaining: {stats['remaining']}."
                )

        elif action == "start_analysis":
            analysis_path = session.get("lp_analysis_path")
            if not analysis_path:
                flash("No analysis file in session. Drop duplicates first or load an existing file.", "warning")
                return redirect(url_for("leadspicker"))
            return redirect(url_for("leadspicker_analyze"))

        elif action == "load_analysis":
            raw_path = request.form.get("analysis_path", "").strip()
            p = Path(raw_path)
            if not p.exists() or not _is_under(Path(config.LP_ANALYSES_DIR), p):
                flash("Invalid analysis file selected.", "error")
                return redirect(url_for("leadspicker"))
            session["lp_analysis_path"] = str(p)
            session.pop("lp_analysis_row_idx", None)  # reset pointer
            flash(f"Loaded analysis file: {p.name}", "info")
            return redirect(url_for("leadspicker_analyze"))

        elif action == "open_draft_messages":
            chosen = request.form.get("analysis_for_draft", "") or current_analysis_path
            if not chosen:
                flash("No analyzed file available. Analyze first.", "warning")
                return redirect(url_for("leadspicker"))
            src_path = Path(chosen)
            if not src_path.exists() or not _is_under(Path(config.LP_ANALYSES_DIR), src_path):
                flash("Selected analyzed file not found.", "error")
                return redirect(url_for("leadspicker"))

            df = io_csv.read_csv(src_path)
            if df.empty:
                flash("Analyzed file is empty; nothing to draft.", "warning")
                return redirect(url_for("leadspicker"))
            rel_col = df.get("relevant")
            if rel_col is not None:
                df = df[rel_col.astype(str).str.lower() == "y"].copy()
            if "message_draft" not in df.columns:
                df["message_draft"] = ""
            out_path = _new_drafted_filename()
            io_csv.write_csv_atomic(out_path, df)
            session["lp_drafts_path"] = str(out_path)
            session["lp_drafts_row_idx"] = 0
            return redirect(url_for("leadspicker_draft_messages"))

        elif action == "push_drafted_wip":
            chosen = request.form.get("drafted_file", "") or latest_drafted
            project_id_str = (request.form.get("project_id") or "").strip()
            if not chosen:
                flash("Select a drafted messages file first.", "warning")
                return redirect(url_for("leadspicker"))
            if not project_id_str.isdigit():
                flash("Enter a numeric Leadspicker project ID.", "warning")
                return redirect(url_for("leadspicker"))

            project_id = int(project_id_str)
            path = config.LP_DRAFTED_DIR / chosen
            if not path.exists():
                flash("Drafted file not found.", "error")
                return redirect(url_for("leadspicker"))

            df = io_csv.read_csv(path)
            # WIP: limit to first 50 to avoid accidental bulk pushes during testing
            summary = leadspicker_api.push_drafted_dataframe(df, project_id, limit=50)
            flash(f"WIP push: sent {summary['sent']}, ok {summary['ok']}, fail {summary['fail']}.", "info")
            if summary.get("errors"):
                flash("Errors (first few): " + " | ".join(summary["errors"]), "warning")

            results = summary.get("results") or []
            ok_indices = [r["index"] for r in results if r.get("ok")]
            if ok_indices:
                subset_all = df.iloc[ok_indices].copy()
                # Append only analyzed rows (relevant in {y,n}) to labeling memory
                subset_mem = subset_all.copy()
                rel_col = subset_mem.get("relevant")
                if rel_col is not None:
                    subset_mem = subset_mem[rel_col.astype(str).str.lower().isin(["y", "n"])]

                if not subset_mem.empty:
                    mem = io_csv.read_lp_labeling_memory()
                    mem_new = pd.concat([mem, subset_mem], ignore_index=True)
                    if "lp_base_post_url" in mem_new.columns:
                        mem_new = mem_new.drop_duplicates(subset=["lp_base_post_url"], keep="first")
                    else:
                        mem_new = mem_new.drop_duplicates(keep="first")
                    io_csv.write_lp_labeling_memory(mem_new)
                    flash(f"Labeling memory updated with {len(subset_mem)} analyzed row(s).", "success")
                else:
                    flash("Labeling memory not updated (no analyzed rows among pushed records).", "info")

                # Append successfully pushed LP rows to master log
                try:
                    stats_master = leadspicker_ingest.append_lp_rows_to_master(df.iloc[ok_indices].copy(), project_id)
                    flash(
                        f"Master log updated: appended {stats_master.get('appended', 0)} "
                        f"of {stats_master.get('total', 0)} row(s), "
                        f"skipped {stats_master.get('skipped_duplicates', 0)} duplicate(s).",
                        "info",
                    )
                except Exception as e:
                    flash(f"Failed to append to master log: {e}", "error")

                airtable_pushed = False
                # Create Airtable records from successfully pushed rows
                try:
                    res_at = airtable_api.create_leadspicker_general_post_records(subset_all)
                    if res_at.get("errors"):
                        flash("Airtable create failed: " + " | ".join(res_at["errors"]), "error")
                    else:
                        airtable_pushed = True
                        msg = f"Airtable create: attempted {res_at['attempted']}, created {res_at['created']}."
                        if res_at.get("details"):
                            msg += " " + " | ".join(res_at["details"])
                        flash(msg, "info")
                except Exception as e:
                    flash(f"Airtable create failed: {e}", "error")

                # Persist push metadata back to drafted file
                today = datetime.now().strftime("%Y-%m-%d")
                if "global_pushed_leadspicker_at" not in df.columns:
                    df["global_pushed_leadspicker_at"] = ""
                if "global_pushed_airtable_at" not in df.columns:
                    df["global_pushed_airtable_at"] = ""
                if "global_status" not in df.columns:
                    df["global_status"] = ""
                idx_labels = df.index[ok_indices]
                df.loc[idx_labels, "global_pushed_leadspicker_at"] = today
                if airtable_pushed:
                    df.loc[idx_labels, "global_pushed_airtable_at"] = today
                df.loc[idx_labels, "global_status"] = "contacted"
                io_csv.write_csv_atomic(path, df)
            else:
                flash("Labeling memory not updated because no rows were pushed successfully.", "warning")
            return redirect(url_for("leadspicker"))

    # GET or fall-through POST render
    return render_template(
        "leadspicker/leadspicker.html",
        recent_analysis_files=recent_analysis_files,
        current_analysis_name=current_analysis_name,
        message=message,
        columns=columns,
        rows=rows,
        projects=projects,
        ready_to_analyze=session.get("lp_ready_to_analyze", False),
        drafted_files=drafted_names,
        latest_drafted=latest_drafted,
        config=config,
        lp_mode="general",
        analysis_dir=config.LP_ANALYZED_DIR,
    )


@app.route("/leadspicker/analyze", methods=["GET", "POST"])
def leadspicker_analyze() -> ResponseReturnValue:
    analysis_path = session.get("lp_analysis_path")
    if not analysis_path:
        flash("No analysis file selected. Please start or load an analysis from the Leadspicker page.", "warning")
        return redirect(url_for("leadspicker"))

    p = Path(analysis_path)
    if not p.exists() or not _is_under(Path(config.LP_ANALYSES_DIR), p):
        flash("Analysis file is missing or invalid. Load a different file.", "error")
        return redirect(url_for("leadspicker"))

    # Load master once per session
    _ = _get_master_df()

    df = io_csv.read_lp_analysis(p)
    counts = _label_counts(df)
    enrich_open = bool(session.get("lp_enrich_open", False))

    # Determine current index to show
    stored_idx = session.get("lp_analysis_row_idx")
    try:
        curr_idx = int(request.form.get("row_idx", stored_idx if stored_idx is not None else -1))
    except Exception:
        curr_idx = -1

    if curr_idx < 0 or curr_idx >= len(df):
        # For Czech pipeline, treat CC as a completed label
        first_unl = _first_unlabeled_index(df, start=0, done_values={"y", "n", "cc"})
        curr_idx = first_unl if first_unl is not None else (len(df) - 1 if len(df) > 0 else 0)

    action = request.form.get("action", "").strip().lower()

    def _save_and_redirect(next_idx: Optional[int]) -> ResponseReturnValue:
        if next_idx is None:
            next_idx = curr_idx
        session["lp_analysis_row_idx"] = int(next_idx)
        return redirect(url_for("leadspicker_analyze"))

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        # Clear search on navigation/label actions
        if action in {"label_y", "label_n", "label_n_learning", "skip", "prev"}:
            _reset_master_search()

        # --- NEW: handle master search ---
        if action == "master_search":
            session["lp_master_query"] = (request.form.get("master_query") or "").strip()
            return redirect(url_for("leadspicker_analyze"))

        if action == "label_y" and 0 <= curr_idx < len(df):
            # Mark relevant = y and open enrich panel on the same row
            df.at[df.index[curr_idx], "relevant"] = "y"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            session["lp_enrich_open"] = True
            # stay on current row
            return _save_and_redirect(curr_idx)

        if action == "label_n" and 0 <= curr_idx < len(df):
            df.at[df.index[curr_idx], "relevant"] = "n"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            session["lp_enrich_open"] = False
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1)
            return _save_and_redirect(next_idx if next_idx is not None else curr_idx)

        if action == "label_n_learning" and 0 <= curr_idx < len(df):
            df.at[df.index[curr_idx], "relevant"] = "n"
            if "learning_data" not in df.columns:
                df["learning_data"] = ""
            df.at[df.index[curr_idx], "learning_data"] = "y"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            session["lp_enrich_open"] = False
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1)
            return _save_and_redirect(next_idx if next_idx is not None else curr_idx)

        if action in {"save_enrich", "save_enrich_stay"} and 0 <= curr_idx < len(df):
            # Read form fields
            full_name = request.form.get("enrich_full_name", "").strip()
            linkedin  = request.form.get("enrich_linkedin", "").strip()
            relation  = request.form.get("enrich_relation", "").strip()
            comp_name = request.form.get("enrich_company_name", "").strip()
            website   = request.form.get("enrich_company_website", "").strip()
            comp_li   = request.form.get("enrich_company_linkedin", "").strip()

            first, last = _split_full_name(full_name)

            # Ensure columns exist
            for col in [
                "global_first_name", "global_last_name", "global_linkedin_url",
                "global_relation_to_the_company", "global_company_name_raw",
                "global_domain_norm", "global_company_linkedin",
                "lp_company_linkedin", "lp_company_linkedin_cleaned",
            ]:
                if col not in df.columns:
                    df[col] = ""

            # Save mapped values
            idx = df.index[curr_idx]
            df.at[idx, "global_first_name"] = first
            df.at[idx, "global_last_name"] = last
            df.at[idx, "global_linkedin_url"] = linkedin
            df.at[idx, "global_relation_to_the_company"] = relation
            df.at[idx, "global_company_name_raw"] = comp_name
            df.at[idx, "global_domain_norm"] = website
            df.at[idx, "global_company_linkedin"] = comp_li
            if comp_li:
                cleaned = comp_li.rstrip("/")
                df.at[idx, "lp_company_linkedin"] = comp_li
                df.at[idx, "lp_company_linkedin_cleaned"] = cleaned

            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save enrichment: {e}", "error")
                return _save_and_redirect(curr_idx)

            if action == "save_enrich_stay":
                session["lp_enrich_open"] = True
                return _save_and_redirect(curr_idx)
            else:
                # Close enrich and go to next record
                session["lp_enrich_open"] = False
                next_idx = curr_idx + 1 if (curr_idx + 1) < len(df) else curr_idx
                return _save_and_redirect(next_idx)

        if action == "finish_labeling":
            try:
                # Save the latest analyzed file
                io_csv.write_lp_analysis(p, df)

                # Append analyzed rows (relevant in {y,n,CC}) to labeling memory
                rel_series = df.get("relevant")
                if rel_series is not None:
                    labeled_mask = rel_series.astype(str).str.lower().isin(["y", "n", "cc"])
                    labeled = df.loc[labeled_mask].copy()
                else:
                    labeled = pd.DataFrame()

                if not labeled.empty:
                    mem = io_csv.read_lp_labeling_memory()
                    mem_new = pd.concat([mem, labeled], ignore_index=True)
                    if "lp_base_post_url" in mem_new.columns:
                        mem_new = mem_new.drop_duplicates(subset=["lp_base_post_url"], keep="first")
                    else:
                        mem_new = mem_new.drop_duplicates(keep="first")
                    io_csv.write_lp_labeling_memory(mem_new)
                    flash(f"Labeled rows appended to labeling memory ({len(labeled)} rows).", "success")
                else:
                    flash("No labeled rows to append to labeling memory.", "info")
            except Exception as e:
                flash(f"Finish labeling failed: {e}", "error")

            session["lp_enrich_open"] = False
            return redirect(url_for("leadspicker"))

        # Go to previous row
        if action == "prev":
            session["lp_enrich_open"] = False
            prev_idx = max(0, curr_idx - 1)
            return _save_and_redirect(prev_idx)

        # Skip to next (prefer next unlabeled; fallback to next row)
        if action == "skip":
            session["lp_enrich_open"] = False
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1)
            if next_idx is None:
                next_idx = min(curr_idx + 1, len(df) - 1)
            return _save_and_redirect(next_idx)

    # Prepare data for rendering
    row_dict = df.iloc[curr_idx].to_dict() if 0 <= curr_idx < len(df) else {}

    # Build master search results (exact + partial match)
    master_query = (session.get("lp_master_query") or "").strip()
    master_results: list[dict] = []
    master_partial_results: list[dict] = []
    if master_query:
        mdf = _get_master_df()
        if not mdf.empty and "global_company_name_norm" in mdf.columns:
            qn = _norm_company_name(master_query)
            cols = [
                "global_company_name_raw",
                "global_company_name_norm",
                "global_domain_norm",
                "global_status",
                "global_first_seen_at",
                "global_last_seen_at",
                "global_leadspicker_project",
                "global_leadspicker_contact_id",
            ]
            show_cols = [c for c in cols if c in mdf.columns]

            # Exact on normalized (lowercased/trimmed)
            exact_mask = mdf["global_company_name_norm"].astype(str) == qn
            if exact_mask.any():
                master_results = mdf.loc[exact_mask, show_cols].head(10).to_dict(orient="records")

            # Partial: normalized contains OR raw contains (case-insensitive)
            part_norm = mdf["global_company_name_norm"].astype(str).str.contains(qn, na=False, regex=False)
            if "global_company_name_raw" in mdf.columns:
                part_raw = mdf["global_company_name_raw"].astype(str).str.contains(master_query, case=False, na=False, regex=False)
                part_mask = part_norm | part_raw
            else:
                part_mask = part_norm

            # Exclude exact rows (avoid duplicates in the partial table)
            part_mask = part_mask & (~exact_mask)

            if part_mask.any():
                master_partial_results = mdf.loc[part_mask, show_cols].head(10).to_dict(orient="records")

            # Debug counts in server log
            try:
                print(f"[master-search] query='{master_query}' qn='{qn}' exact={int(exact_mask.sum())} partial={int(part_mask.sum())}")
            except Exception:
                pass

    master_count = len(master_results)
    master_has_match = master_count > 0
    master_partial_count = len(master_partial_results)

    return render_template(
        "leadspicker/analyze.html",
        analysis_path=str(p),
        row_idx=int(curr_idx),
        total_rows=int(len(df)),
        counts=counts,
        row=row_dict,
        columns=list(df.columns),
        progress={
            "index": (int(curr_idx) + 1) if len(df) > 0 else 0,
            "total": int(len(df)),
            "y": counts.get("y", 0),
            "n": counts.get("n", 0),
            "unlabeled": counts.get("unlabeled", 0),
        },
        enrich_open=enrich_open,
        master_query=master_query,
        master_results=master_results,
        master_has_match=master_has_match,
        master_count=master_count,
        master_partial_results=master_partial_results,
        master_partial_count=master_partial_count,
        lp_mode="general",
    )


@app.route("/leadspicker/czech", methods=["GET", "POST"])
def leadspicker_czech() -> ResponseReturnValue:
    """
    Czech-focused Leadspicker pipeline: same flow as leadspicker(),
    but using the expansion_czechia_post subfolders and separate labeling memory.
    """
    columns, rows, message = [], [], ""
    projects = []
    recent_analysis_files = [
        p for p in (config.LP_CZECHIA_ANALYZED_DIR.glob("*.csv") if config.LP_CZECHIA_ANALYZED_DIR.exists() else [])
    ]
    recent_analysis_files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    recent_analysis_files = recent_analysis_files[:20]

    current_analysis_path = session.get("lp_cz_analysis_path")
    current_analysis_name = Path(current_analysis_path).name if current_analysis_path else None

    drafted_files = _list_drafted_files_for_dir(config.LP_CZECHIA_DRAFTED_DIR)
    drafted_names = [p.name for p in drafted_files]
    latest_drafted = drafted_names[0] if drafted_names else ""

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        ts = datetime.now().strftime("%Y-%m-%d_%H%M")
        source_batch_id = f"LP-{datetime.now().strftime('%d-%m-%Y')}"
        staged_norm = config.LP_CZECHIA_NORMALIZED_DIR / f"LP_{ts}_normalized.csv"

        if action == "list_projects":
            session_obj, csrf, debug = leadspicker_api.get_session_and_csrf()
            if not session_obj or not csrf:
                message = f"Failed to start session or retrieve CSRF. {debug}"
            else:
                projects = leadspicker_api.get_project_ids(session_obj, csrf)
                message = f"Found {len(projects)} projects." if projects else "No projects returned."

        elif action == "api_fetch":
            raw_ids = request.form.get("project_ids", "11870")
            project_ids = [p.strip() for p in raw_ids.split(",") if p.strip()]
            df_raw = fetch_api(project_ids=project_ids, since_date="")
            if df_raw is None or df_raw.empty:
                message = "No records returned from API for given project ID(s)."
            else:
                raw_path = config.LP_CZECHIA_FETCH_DIR / f"LP_{ts}_raw.csv"
                df_raw.to_csv(raw_path, sep=";", index=False, encoding="utf-8")
                df_norm = normalize_to_master_like(df_raw, source_batch_id)
                df_norm = align_to_master_schema(df_norm)
                df_norm.to_csv(staged_norm, sep=";", index=False, encoding="utf-8")
                session["lp_cz_current_stage"] = str(staged_norm)
                session["lp_cz_ready_to_analyze"] = False
                session.pop("lp_cz_analyze_idx", None)
                session.pop("lp_cz_analysis_file", None)

                columns, rows = df_norm.columns.tolist(), df_norm.head(20).to_dict("records")
                message = f"Fetched {len(df_norm)} records from project(s): {', '.join(project_ids)}. Ready to drop duplicates."

        elif action == "manual":
            file = request.files.get("file")
            if file and file.filename:
                raw_path = config.LP_CZECHIA_FETCH_DIR / f"LP_{ts}_raw.csv"
                file.save(raw_path)
                df_raw = load_manual_csv(raw_path)
                if df_raw is None or df_raw.empty:
                    message = "Uploaded file is empty or unreadable."
                else:
                    df_norm = normalize_to_master_like(df_raw, source_batch_id)
                    df_norm = align_to_master_schema(df_norm)
                    df_norm.to_csv(staged_norm, sep=";", index=False, encoding="utf-8")
                    session["lp_cz_current_stage"] = str(staged_norm)
                    session["lp_cz_ready_to_analyze"] = False
                    session.pop("lp_cz_analyze_idx", None)
                    session.pop("lp_cz_analysis_file", None)

                    columns, rows = df_norm.columns.tolist(), df_norm.head(20).to_dict("records")
                    message = f"Loaded {len(df_norm)} records from upload. Ready to drop duplicates."
            else:
                message = "Please choose a CSV file to upload."

        elif action == "drop_dupes":
            staged_path = session.get("lp_cz_current_stage")
            if not staged_path or not Path(staged_path).exists():
                message = "No staged data found. Please import first."
            else:
                df_stage = pd.read_csv(staged_path, sep=";", dtype=str, keep_default_na=False).fillna("")
                df_mem = io_csv.read_lp_czechia_labeling_memory()
                df_dedup, stats = drop_dupes_against_lp_memory(df_stage, df_mem)

                staged_dedup = config.LP_CZECHIA_DROPPED_DIR / f"{Path(staged_path).stem}_deduped.csv"
                df_dedup.to_csv(staged_dedup, sep=";", index=False, encoding="utf-8")
                session["lp_cz_current_stage"] = str(staged_dedup)
                session["lp_cz_ready_to_analyze"] = True
                session["lp_cz_analyze_idx"] = 0
                session.pop("lp_cz_analysis_file", None)

                try:
                    analysis_path = leadspicker_ingest.save_lp_analysis_from_df(
                        df_dedup, analyses_dir=config.LP_CZECHIA_ANALYZED_DIR
                    )
                    session["lp_cz_analysis_path"] = str(analysis_path)
                    session.pop("lp_cz_analysis_row_idx", None)
                    flash(f"Analysis file created: {Path(analysis_path).name}", "info")
                except Exception as e:
                    flash(f"Failed to create analysis file: {e}", "error")

                columns = df_dedup.columns.tolist()
                rows = df_dedup.head(20).to_dict("records")
                message = (
                    f"Dropped {stats['dropped']} / {stats['incoming']} posts already in labeling memory. "
                    f"Remaining: {stats['remaining']}."
                )

        elif action == "start_analysis":
            analysis_path = session.get("lp_cz_analysis_path")
            if not analysis_path:
                flash("No analysis file in session. Drop duplicates first or load an existing file.", "warning")
                return redirect(url_for("leadspicker_czech"))
            return redirect(url_for("leadspicker_analyze_czech"))

        elif action == "load_analysis":
            raw_path = request.form.get("analysis_path", "").strip()
            p = Path(raw_path)
            base = config.LP_CZECHIA_ANALYZED_DIR
            if not p.exists() or not _is_under(base, p):
                flash("Invalid analysis file selected.", "error")
                return redirect(url_for("leadspicker_czech"))
            session["lp_cz_analysis_path"] = str(p)
            session.pop("lp_cz_analysis_row_idx", None)
            flash(f"Loaded analysis file: {p.name}", "info")
            return redirect(url_for("leadspicker_analyze_czech"))

        elif action == "open_draft_messages":
            chosen = request.form.get("analysis_for_draft", "") or current_analysis_path
            if not chosen:
                flash("No analyzed file available. Analyze first.", "warning")
                return redirect(url_for("leadspicker_czech"))
            src_path = Path(chosen)
            base = config.LP_CZECHIA_ANALYZED_DIR
            if not src_path.exists() or not _is_under(base, src_path):
                flash("Selected analyzed file not found.", "error")
                return redirect(url_for("leadspicker_czech"))

            df = io_csv.read_csv(src_path)
            if df.empty:
                flash("Analyzed file is empty; nothing to draft.", "warning")
                return redirect(url_for("leadspicker_czech"))
            rel_col = df.get("relevant")
            if rel_col is not None:
                df = df[rel_col.astype(str).str.lower() == "y"].copy()
            if "message_draft" not in df.columns:
                df["message_draft"] = ""
            out_path = _new_drafted_filename_for_dir(config.LP_CZECHIA_DRAFTED_DIR)
            io_csv.write_csv_atomic(out_path, df)
            # Reuse the main drafting UI; path itself is Czech-specific
            session["lp_drafts_path"] = str(out_path)
            session["lp_drafts_row_idx"] = 0
            return redirect(url_for("leadspicker_draft_messages"))

        elif action == "push_drafted_wip":
            chosen = request.form.get("drafted_file", "") or latest_drafted
            project_id_str = (request.form.get("project_id") or "").strip()
            if not chosen:
                flash("Select a drafted messages file first.", "warning")
                return redirect(url_for("leadspicker_czech"))
            if not project_id_str.isdigit():
                flash("Enter a numeric Leadspicker project ID.", "warning")
                return redirect(url_for("leadspicker_czech"))

            project_id = int(project_id_str)
            path = config.LP_CZECHIA_DRAFTED_DIR / chosen
            if not path.exists():
                flash("Drafted file not found.", "error")
                return redirect(url_for("leadspicker_czech"))

            df = io_csv.read_csv(path)
            summary = leadspicker_api.push_drafted_dataframe(df, project_id, limit=50)
            flash(f"WIP push: sent {summary['sent']}, ok {summary['ok']}, fail {summary['fail']}.", "info")
            if summary.get("errors"):
                flash("Errors (first few): " + " | ".join(summary["errors"]), "warning")

            results = summary.get("results") or []
            ok_indices = [r["index"] for r in results if r.get("ok")]
            if ok_indices:
                subset_all = df.iloc[ok_indices].copy()
                subset_mem = subset_all.copy()
                rel_col = subset_mem.get("relevant")
                if rel_col is not None:
                    subset_mem = subset_mem[rel_col.astype(str).str.lower().isin(["y", "n"])]

                if not subset_mem.empty:
                    mem = io_csv.read_lp_czechia_labeling_memory()
                    mem_new = pd.concat([mem, subset_mem], ignore_index=True)
                    if "lp_base_post_url" in mem_new.columns:
                        mem_new = mem_new.drop_duplicates(subset=["lp_base_post_url"], keep="first")
                    else:
                        mem_new = mem_new.drop_duplicates(keep="first")
                    io_csv.write_lp_czechia_labeling_memory(mem_new)
                    flash(f"Labeling memory updated with {len(subset_mem)} analyzed row(s).", "success")
                else:
                    flash("Labeling memory not updated (no analyzed rows among pushed records).", "info")

                try:
                    stats_master = leadspicker_ingest.append_lp_rows_to_master(df.iloc[ok_indices].copy(), project_id)
                    flash(
                        f"Master log updated: appended {stats_master.get('appended', 0)} "
                        f"of {stats_master.get('total', 0)} row(s), "
                        f"skipped {stats_master.get('skipped_duplicates', 0)} duplicate(s).",
                        "info",
                    )
                except Exception as e:
                    flash(f"Failed to append to master log: {e}", "error")

                airtable_pushed = False
                # Create Airtable records from successfully pushed rows
                try:
                    res_at = airtable_api.create_leadspicker_czech_post_records(subset_all)
                    if res_at.get("errors"):
                        flash("Airtable create failed: " + " | ".join(res_at["errors"]), "error")
                    else:
                        airtable_pushed = True
                        msg = f"Airtable create: attempted {res_at['attempted']}, created {res_at['created']}."
                        if res_at.get("details"):
                            msg += " " + " | ".join(res_at["details"])
                        flash(msg, "info")
                except Exception as e:
                    flash(f"Airtable create failed: {e}", "error")

                today = datetime.now().strftime("%Y-%m-%d")
                if "global_pushed_leadspicker_at" not in df.columns:
                    df["global_pushed_leadspicker_at"] = ""
                if "global_pushed_airtable_at" not in df.columns:
                    df["global_pushed_airtable_at"] = ""
                if "global_status" not in df.columns:
                    df["global_status"] = ""
                idx_labels = df.index[ok_indices]
                df.loc[idx_labels, "global_pushed_leadspicker_at"] = today
                if airtable_pushed:
                    df.loc[idx_labels, "global_pushed_airtable_at"] = today
                df.loc[idx_labels, "global_status"] = "contacted"
                io_csv.write_csv_atomic(path, df)
            else:
                flash("Labeling memory not updated because no rows were pushed successfully.", "warning")
            return redirect(url_for("leadspicker_czech"))

    return render_template(
        "leadspicker/leadspicker.html",
        recent_analysis_files=recent_analysis_files,
        current_analysis_name=current_analysis_name,
        message=message,
        columns=columns,
        rows=rows,
        projects=projects,
        ready_to_analyze=session.get("lp_cz_ready_to_analyze", False),
        drafted_files=drafted_names,
        latest_drafted=latest_drafted,
        config=config,
        lp_mode="czech",
        analysis_dir=config.LP_CZECHIA_ANALYZED_DIR,
    )


@app.route("/leadspicker/czech/analyze", methods=["GET", "POST"])
def leadspicker_analyze_czech() -> ResponseReturnValue:
    analysis_path = session.get("lp_cz_analysis_path")
    if not analysis_path:
        flash("No analysis file selected. Please start or load an analysis from the Leadspicker Czech page.", "warning")
        return redirect(url_for("leadspicker_czech"))

    p = Path(analysis_path)
    base = config.LP_CZECHIA_ANALYZED_DIR
    if not p.exists() or not _is_under(base, p):
        flash("Analysis file is missing or invalid. Load a different file.", "error")
        return redirect(url_for("leadspicker_czech"))

    _ = _get_master_df()

    df = io_csv.read_lp_analysis(p)
    counts = _label_counts(df)
    enrich_open = bool(session.get("lp_cz_enrich_open", False))

    stored_idx = session.get("lp_cz_analysis_row_idx")
    try:
        curr_idx = int(request.form.get("row_idx", stored_idx if stored_idx is not None else -1))
    except Exception:
        curr_idx = -1

    if curr_idx < 0 or curr_idx >= len(df):
        first_unl = _first_unlabeled_index(df, start=0)
        curr_idx = first_unl if first_unl is not None else (len(df) - 1 if len(df) > 0 else 0)

    action = request.form.get("action", "").strip().lower()

    def _save_and_redirect(next_idx: Optional[int]) -> ResponseReturnValue:
        if next_idx is None:
            next_idx = curr_idx
        session["lp_cz_analysis_row_idx"] = int(next_idx)
        return redirect(url_for("leadspicker_analyze_czech"))

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action in {"label_y", "label_n", "label_n_learning", "label_cc", "skip", "prev"}:
            _reset_master_search_cz()

        if action == "master_search":
            session["lp_cz_master_query"] = (request.form.get("master_query") or "").strip()
            return redirect(url_for("leadspicker_analyze_czech"))

        if action == "label_y" and 0 <= curr_idx < len(df):
            df.at[df.index[curr_idx], "relevant"] = "y"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            session["lp_cz_enrich_open"] = True
            return _save_and_redirect(curr_idx)

        if action == "label_n" and 0 <= curr_idx < len(df):
            df.at[df.index[curr_idx], "relevant"] = "n"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            session["lp_cz_enrich_open"] = False
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1, done_values={"y", "n", "cc"})
            return _save_and_redirect(next_idx if next_idx is not None else curr_idx)

        if action == "label_n_learning" and 0 <= curr_idx < len(df):
            df.at[df.index[curr_idx], "relevant"] = "n"
            if "learning_data" not in df.columns:
                df["learning_data"] = ""
            df.at[df.index[curr_idx], "learning_data"] = "y"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            session["lp_cz_enrich_open"] = False
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1, done_values={"y", "n", "cc"})
            return _save_and_redirect(next_idx if next_idx is not None else curr_idx)

        if action == "label_cc" and 0 <= curr_idx < len(df):
            # Compliance Checkpoint: same behavior as YES,
            # but mark relevant as 'CC' instead of 'y'.
            df.at[df.index[curr_idx], "relevant"] = "CC"
            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            # Open enrich panel, stay on current row
            session["lp_cz_enrich_open"] = True
            return _save_and_redirect(curr_idx)

        if action == "open_enrich" and 0 <= curr_idx < len(df):
            session["lp_cz_enrich_open"] = True
            return _save_and_redirect(curr_idx)

        if action == "close_enrich":
            session["lp_cz_enrich_open"] = False
            return _save_and_redirect(curr_idx)

        if action in {"save_enrich", "save_enrich_stay"} and 0 <= curr_idx < len(df):
            pre_company = (df.at[df.index[curr_idx], "lp_company_name"]
                           if "lp_company_name" in df.columns else "")
            pre_company_li = (df.at[df.index[curr_idx], "lp_company_linkedin"]
                              if "lp_company_linkedin" in df.columns else "")
            pre_company_li_cleaned = (df.at[df.index[curr_idx], "lp_company_linkedin_cleaned"]
                                      if "lp_company_linkedin_cleaned" in df.columns else pre_company_li)
            pre_website = (df.at[df.index[curr_idx], "lp_company_website"]
                           if "lp_company_website" in df.columns else "")
            pre_first = (df.at[df.index[curr_idx], "lp_lead_first_name"]
                         if "lp_lead_first_name" in df.columns else "")
            pre_last = (df.at[df.index[curr_idx], "lp_lead_last_name"]
                        if "lp_lead_last_name" in df.columns else "")
            pre_name = f"{pre_first} {pre_last}".strip()
            pre_linkedin = (df.at[df.index[curr_idx], "lp_contacted_lead_linkedin"]
                            if "lp_contacted_lead_linkedin" in df.columns else "")
            pre_relation = (df.at[df.index[curr_idx], "lp_relation_to_the_company"]
                            if "lp_relation_to_the_company" in df.columns else "")

            company_name = request.form.get("enrich_company_name") or pre_company
            company_linkedin = request.form.get("enrich_company_linkedin") or pre_company_li
            company_linkedin_cleaned = request.form.get("enrich_company_linkedin_cleaned") or pre_company_li_cleaned
            company_website = request.form.get("enrich_company_website") or pre_website
            full_name = request.form.get("enrich_full_name") or pre_name
            linkedin_person = request.form.get("enrich_linkedin") or pre_linkedin
            relation = request.form.get("enrich_relation") or pre_relation

            first_name = full_name.strip()
            last_name = ""
            if " " in full_name.strip():
                parts = full_name.strip().split()
                first_name = parts[0]
                last_name = " ".join(parts[1:])

            for col in [
                "global_first_name", "global_last_name", "global_linkedin_url",
                "global_relation_to_the_company", "global_company_name_raw",
                "global_domain_norm", "global_company_linkedin",
                "lp_company_linkedin", "lp_company_linkedin_cleaned",
            ]:
                if col not in df.columns:
                    df[col] = ""

            idx = df.index[curr_idx]
            df.at[idx, "global_first_name"] = first_name
            df.at[idx, "global_last_name"] = last_name
            df.at[idx, "global_linkedin_url"] = linkedin_person
            df.at[idx, "global_relation_to_the_company"] = relation
            df.at[idx, "global_company_name_raw"] = company_name
            df.at[idx, "global_domain_norm"] = company_website
            df.at[idx, "global_company_linkedin"] = company_linkedin
            if company_linkedin:
                cleaned = company_linkedin.rstrip("/")
                df.at[idx, "lp_company_linkedin"] = company_linkedin
                df.at[idx, "lp_company_linkedin_cleaned"] = cleaned

            try:
                io_csv.write_lp_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save enrichment: {e}", "error")
                return _save_and_redirect(curr_idx)

            if action == "save_enrich_stay":
                session["lp_cz_enrich_open"] = True
                return _save_and_redirect(curr_idx)
            else:
                session["lp_cz_enrich_open"] = False
                next_idx = curr_idx + 1 if (curr_idx + 1) < len(df) else curr_idx
                return _save_and_redirect(next_idx)

        if action == "finish_labeling":
            try:
                io_csv.write_lp_analysis(p, df)

                rel_series = df.get("relevant")
                if rel_series is not None:
                    labeled_mask = rel_series.astype(str).str.lower().isin(["y", "n", "cc"])
                    labeled = df.loc[labeled_mask].copy()
                else:
                    labeled = pd.DataFrame()

                if not labeled.empty:
                    mem = io_csv.read_lp_czechia_labeling_memory()
                    mem_new = pd.concat([mem, labeled], ignore_index=True)
                    if "lp_base_post_url" in mem_new.columns:
                        mem_new = mem_new.drop_duplicates(subset=["lp_base_post_url"], keep="first")
                    else:
                        mem_new = mem_new.drop_duplicates(keep="first")
                    io_csv.write_lp_czechia_labeling_memory(mem_new)
                    flash(f"Labeled rows appended to labeling memory ({len(labeled)} rows).", "success")
                else:
                    flash("No labeled rows to append to labeling memory.", "info")
            except Exception as e:
                flash(f"Finish labeling failed: {e}", "error")

            session["lp_cz_enrich_open"] = False
            return redirect(url_for("leadspicker_czech"))

        if action == "prev":
            session["lp_cz_enrich_open"] = False
            prev_idx = max(0, curr_idx - 1)
            return _save_and_redirect(prev_idx)

        if action == "skip":
            session["lp_cz_enrich_open"] = False
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1, done_values={"y", "n", "cc"})
            if next_idx is None:
                next_idx = min(curr_idx + 1, len(df) - 1)
            return _save_and_redirect(next_idx)

    row_dict = df.iloc[curr_idx].to_dict() if 0 <= curr_idx < len(df) else {}

    master_query = (session.get("lp_cz_master_query") or "").strip()
    master_results: list[dict] = []
    master_partial_results: list[dict] = []
    master_count = 0
    master_partial_count = 0

    if master_query:
        mdf = _get_master_df()
        if not mdf.empty and "global_company_name_norm" in mdf.columns:
            qn = _norm_company_name(master_query)
            cols = [
                "global_company_name_raw",
                "global_company_name_norm",
                "global_domain_norm",
                "global_status",
                "global_first_seen_at",
                "global_last_seen_at",
                "global_leadspicker_project",
                "global_leadspicker_contact_id",
            ]
            show_cols = [c for c in cols if c in mdf.columns]

            exact_mask = mdf["global_company_name_norm"].astype(str) == qn
            if exact_mask.any():
                master_results = mdf.loc[exact_mask, show_cols].head(10).to_dict(orient="records")

            part_norm = mdf["global_company_name_norm"].astype(str).str.contains(qn, na=False, regex=False)
            if "global_company_name_raw" in mdf.columns:
                part_raw = mdf["global_company_name_raw"].astype(str).str.contains(master_query, case=False, na=False, regex=False)
                part_mask = part_norm | part_raw
            else:
                part_mask = part_norm

            part_mask = part_mask & (~exact_mask)

            if part_mask.any():
                master_partial_results = mdf.loc[part_mask, show_cols].head(10).to_dict(orient="records")

            master_count = len(master_results)
            master_partial_count = len(master_partial_results)

    return render_template(
        "leadspicker/analyze.html",
        analysis_path=str(p),
        row_idx=int(curr_idx),
        total_rows=int(len(df)),
        counts=counts,
        row=row_dict,
        columns=list(df.columns),
        progress={
            "index": (int(curr_idx) + 1) if len(df) > 0 else 0,
            "total": int(len(df)),
            "y": counts.get("y", 0),
            "n": counts.get("n", 0),
            "unlabeled": counts.get("unlabeled", 0),
        },
        enrich_open=enrich_open,
        master_query=master_query,
        master_results=master_results,
        master_has_match=master_count > 0,
        master_count=master_count,
        master_partial_results=master_partial_results,
        master_partial_count=master_partial_count,
        lp_mode="czech",
    )


@app.route("/crunchbase", methods=["GET", "POST"])
def crunchbase() -> ResponseReturnValue:
    """
    Initial Crunchbase menu that mirrors the Leadspicker flow.
    Currently supports staging CSV uploads + preview; API fetch is a placeholder.
    """
    message = ""
    columns: list[str] = []
    rows: list[dict] = []
    selected_stage = ""
    def _refresh_stage() -> list[Path]:
        return _list_cb_stage_files(limit=50)

    def _create_analysis_from_df(df: pd.DataFrame) -> str:
        analysis_path = _cb_analysis_filename()
        analysis_path.parent.mkdir(parents=True, exist_ok=True)
        io_csv.write_cb_analysis(analysis_path, df)
        session["cb_analysis_path"] = str(analysis_path)
        session["cb_analysis_row_idx"] = 0
        session["cb_ready_to_analyze"] = True
        return analysis_path.name

    stage_files = _refresh_stage()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()
        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        if action in {"airtable_fetch", "api_fetch"}:
            status_filter = (request.form.get("status_filter") or "Longlist").strip()
            view = (request.form.get("view") or "").strip() or None
            max_records_raw = (request.form.get("max_records") or "").strip()
            contact_enriched_zero = bool(request.form.get("contact_enriched_zero"))
            max_records = None
            if max_records_raw:
                try:
                    max_records = int(max_records_raw)
                except ValueError:
                    message = "max_records must be an integer."
                    max_records = None

            try:
                df = airtable_api.fetch_crunchbase_source(
                    status_filter=status_filter or None,
                    view=view,
                    max_records=max_records,
                    contact_enriched_zero=contact_enriched_zero,
                )
                if df is None or df.empty:
                    message = "No records returned from Airtable."
                else:
                    staged_path = config.CB_FETCH_DIR / f"CB_airtable_{ts}.csv"
                    df.to_csv(staged_path, sep=";", index=False, encoding="utf-8")
                    columns = df.columns.tolist()
                    rows = df.head(20).to_dict("records")
                    selected_stage = staged_path.name
                    session["cb_current_stage"] = str(staged_path)
                    analysis_name = _create_analysis_from_df(df)
                    filter_bits = []
                    if status_filter:
                        filter_bits.append(f"Status={status_filter}")
                    if contact_enriched_zero:
                        filter_bits.append("Contact enriched=0")
                    filter_text = "; ".join(filter_bits) if filter_bits else "No filter"
                    message = (
                        f"Fetched {len(df)} records from Airtable "
                        f"({filter_text}; View={view or 'default'}). "
                        f"Saved to {staged_path.name}. Analysis: {analysis_name}."
                    )
                    stage_files = _refresh_stage()
            except Exception as e:
                message = f"Airtable fetch failed: {e}"

        elif action == "manual":
            file = request.files.get("file")
            if file and file.filename:
                staged_path = config.CB_FETCH_DIR / f"CB_{ts}_raw.csv"
                file.save(staged_path)
                try:
                    df = pd.read_csv(staged_path, sep=";", dtype=str, keep_default_na=False).fillna("")
                    columns = df.columns.tolist()
                    rows = df.head(20).to_dict("records")
                    selected_stage = staged_path.name
                    session["cb_current_stage"] = str(staged_path)
                    analysis_name = _create_analysis_from_df(df)
                    message = f"Uploaded {len(df)} rows from {file.filename}. Saved to {staged_path.name}. Analysis: {analysis_name}."
                except Exception as e:
                    message = f"Saved file to {staged_path.name} but failed to preview: {e}"
                stage_files = _refresh_stage()
            else:
                message = "Please choose a CSV file to upload."

        elif action == "load_stage":
            chosen = (request.form.get("stage_file") or "").strip()
            if not chosen:
                message = "Select a staged file to preview."
            else:
                p = config.CB_FETCH_DIR / chosen
                if p.exists() and _is_under(config.CB_FETCH_DIR, p):
                    try:
                        df = pd.read_csv(p, sep=";", dtype=str, keep_default_na=False).fillna("")
                        columns = df.columns.tolist()
                        rows = df.head(20).to_dict("records")
                        selected_stage = chosen
                        session["cb_current_stage"] = str(p)
                        analysis_name = _create_analysis_from_df(df)
                        message = f"Loaded staged file {chosen}. Analysis: {analysis_name}."
                    except Exception as e:
                        message = f"Failed to read {chosen}: {e}"
                else:
                    message = "Selected staged file not found."

        elif action == "start_analysis":
            analysis_path = session.get("cb_analysis_path")
            if not analysis_path:
                flash("No analysis file in session. Import/fetch to create one or load an existing file.", "warning")
                return redirect(url_for("crunchbase"))
            return redirect(url_for("crunchbase_analyze"))

        elif action == "load_analysis":
            raw_path = request.form.get("analysis_path", "").strip()
            p = Path(raw_path)
            if not p.exists() or not _is_under(Path(config.CB_ANALYSES_DIR), p):
                flash("Invalid analysis file selected.", "error")
                return redirect(url_for("crunchbase"))
            session["cb_analysis_path"] = str(p)
            session.pop("cb_analysis_row_idx", None)
            session["cb_ready_to_analyze"] = True
            flash(f"Loaded analysis file: {p.name}", "info")
            return redirect(url_for("crunchbase_analyze"))

        elif action == "push_updates":
            analysis_path = session.get("cb_analysis_path")
            if not analysis_path:
                flash("No analysis file in session. Import/fetch to create one first.", "warning")
                return redirect(url_for("crunchbase"))
            p = Path(analysis_path)
            if not p.exists():
                flash("Analysis file missing on disk.", "error")
                return redirect(url_for("crunchbase"))

            df = io_csv.read_cb_analysis(p)
            if df.empty:
                flash("Analysis file is empty; nothing to push.", "warning")
                return redirect(url_for("crunchbase"))

            push_airtable = bool(request.form.get("push_airtable"))
            push_lp = bool(request.form.get("push_leadspicker"))
            lp_project_raw = (request.form.get("lp_project_id") or "18009").strip()

            if push_airtable:
                res = airtable_api.update_crunchbase_records(df)
                if res.get("errors"):
                    flash("Airtable push failed: " + " | ".join(res["errors"]), "error")
                else:
                    msg = f"Airtable push: attempted {res['attempted']}, updated {res['updated']}."
                    if res.get("details"):
                        msg += " " + " | ".join(res["details"])
                    flash(msg, "info")

            if push_lp:
                if not lp_project_raw.isdigit():
                    flash("Leadspicker project ID must be numeric.", "error")
                    return redirect(url_for("crunchbase"))
                lp_project_id = int(lp_project_raw)
                # Only push rows marked as Quality B - Contacted
                df_lp_source = df.copy()
                if "Status" in df_lp_source.columns:
                    mask = df_lp_source["Status"].astype(str).str.strip().str.lower() == "quality b - contacted"
                    df_lp_source = df_lp_source[mask].copy()
                else:
                    df_lp_source = df_lp_source.iloc[0:0].copy()  # no status column means push none

                if df_lp_source.empty:
                    flash("No rows with Status='Quality B - Contacted' to push to Leadspicker.", "warning")
                    return redirect(url_for("crunchbase"))

                df_lp = _cb_to_lp_df(df_lp_source)
                # Drop rows without a message draft to avoid empty pushes
                if "message_draft" in df_lp.columns:
                    df_lp = df_lp[df_lp["message_draft"].astype(str).str.strip() != ""]
                if df_lp.empty:
                    flash("Filtered rows have no Message fin; nothing to push to Leadspicker.", "warning")
                    return redirect(url_for("crunchbase"))
                summary = leadspicker_api.push_drafted_dataframe(df_lp, lp_project_id)
                flash(f"Leadspicker push: sent {summary['sent']}, ok {summary['ok']}, fail {summary['fail']}.", "info")
                if summary.get("errors"):
                    flash("Leadspicker errors (first few): " + " | ".join(summary["errors"]), "warning")

                # Append successfully pushed LP rows to master log
                results = summary.get("results") or []
                ok_indices = [r["index"] for r in results if r.get("ok")]
                if ok_indices:
                    try:
                        stats_master = leadspicker_ingest.append_lp_rows_to_master(
                            df_lp.iloc[ok_indices].copy(),
                            lp_project_id,
                        )
                        flash(
                            f"Master log updated: appended {stats_master.get('appended', 0)} "
                            f"of {stats_master.get('total', 0)} row(s), "
                            f"skipped {stats_master.get('skipped_duplicates', 0)} duplicate(s).",
                            "info",
                        )
                    except Exception as e:
                        flash(f"Failed to append to master log: {e}", "error")

            if not push_airtable and not push_lp:
                flash("Select at least one destination (Airtable or Leadspicker).", "warning")
            return redirect(url_for("crunchbase"))

        else:
            message = "Action not recognized."

    # Refresh analysis pointers for render (after any POST updates)
    current_analysis_path = session.get("cb_analysis_path")
    current_analysis_name = Path(current_analysis_path).name if current_analysis_path else None
    ready_to_analyze = bool(session.get("cb_ready_to_analyze", False) or session.get("cb_analysis_path"))
    recent_analysis_files = io_csv.list_cb_analysis_files(limit=20)

    return render_template(
        "crunchbase.html",
        message=message,
        columns=columns,
        rows=rows,
        stage_files=stage_files,
        selected_stage=selected_stage,
        recent_analysis_files=recent_analysis_files,
        current_analysis_name=current_analysis_name,
        ready_to_analyze=ready_to_analyze,
        config=config,
    )


@app.route("/crunchbase/analyze", methods=["GET", "POST"])
def crunchbase_analyze() -> ResponseReturnValue:
    analysis_path = session.get("cb_analysis_path")
    if not analysis_path:
        flash("No analysis file selected. Please start or load an analysis from Crunchbase.", "warning")
        return redirect(url_for("crunchbase"))

    p = Path(analysis_path)
    if not p.exists() or not _is_under(Path(config.CB_ANALYSES_DIR), p):
        flash("Analysis file is missing or invalid. Load a different file.", "error")
        return redirect(url_for("crunchbase"))

    df = io_csv.read_cb_analysis(p)
    counts = _label_counts(df)

    stored_idx = session.get("cb_analysis_row_idx")
    try:
        curr_idx = int(request.form.get("row_idx", stored_idx if stored_idx is not None else -1))
    except Exception:
        curr_idx = -1

    if curr_idx < 0 or curr_idx >= len(df):
        first_unl = _first_unlabeled_index(df, start=0)
        curr_idx = first_unl if first_unl is not None else (len(df) - 1 if len(df) > 0 else 0)

    action = (request.form.get("action") or "").strip().lower()

    def _save_and_redirect(next_idx: Optional[int]) -> ResponseReturnValue:
        if next_idx is None:
            next_idx = curr_idx
        session["cb_analysis_row_idx"] = int(next_idx)
        return redirect(url_for("crunchbase_analyze"))

    def _choose_key(df_ref: pd.DataFrame) -> Optional[str]:
        candidate_keys = ["_id", "Organization Name", "Company", "Organization"]
        for k in candidate_keys:
            if k in df_ref.columns:
                return k
        return df_ref.columns[0] if len(df_ref.columns) > 0 else None

    if request.method == "POST":
        if action == "label_y" and 0 <= curr_idx < len(df):
            df.at[df.index[curr_idx], "relevant"] = "y"
            try:
                io_csv.write_cb_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save label: {e}", "error")
                return _save_and_redirect(curr_idx)
            next_idx = _first_unlabeled_index(df, start=curr_idx + 1)
            return _save_and_redirect(next_idx if next_idx is not None else curr_idx)

        if action in {"save_fields", "save_fields_stay"} and 0 <= curr_idx < len(df):
            message_fin = (request.form.get("message_fin") or "").strip()
            main_contact = (request.form.get("main_contact") or "").strip()
            sc1 = (request.form.get("secondary_contact_1") or "").strip()
            sc2 = (request.form.get("secondary_contact_2") or "").strip()
            sc3 = (request.form.get("secondary_contact_3") or "").strip()

            for col in ["Message fin", "Main Contact", "Secondary Contact #1", "Secondary Contact #2", "Secondary Contact #3", "Status"]:
                if col not in df.columns:
                    df[col] = ""

            idx = df.index[curr_idx]
            df.at[idx, "Message fin"] = message_fin
            df.at[idx, "Main Contact"] = main_contact
            df.at[idx, "Secondary Contact #1"] = sc1
            df.at[idx, "Secondary Contact #2"] = sc2
            df.at[idx, "Secondary Contact #3"] = sc3
            if action == "save_fields":
                # Save & next marks the record as ready for LP push
                df.at[idx, "Status"] = "Quality B - Contacted"

            try:
                io_csv.write_cb_analysis(p, df)
            except Exception as e:
                flash(f"Failed to save fields: {e}", "error")
                return _save_and_redirect(curr_idx)

            if action == "save_fields_stay":
                return _save_and_redirect(curr_idx)
            next_idx = min(curr_idx + 1, len(df) - 1)
            return _save_and_redirect(next_idx)

        if action == "eliminate" and 0 <= curr_idx < len(df):
            if "Status" not in df.columns:
                df["Status"] = ""
            df.at[df.index[curr_idx], "Status"] = "Eliminated"
            try:
                io_csv.write_cb_analysis(p, df)
            except Exception as e:
                flash(f"Failed to eliminate: {e}", "error")
                return _save_and_redirect(curr_idx)
            next_idx = min(curr_idx + 1, len(df) - 1)
            flash("Status set to Eliminated.", "info")
            return _save_and_redirect(next_idx)

        if action == "uneliminate" and 0 <= curr_idx < len(df):
            if "Status" not in df.columns:
                df["Status"] = ""
            df.at[df.index[curr_idx], "Status"] = "Longlist"
            try:
                io_csv.write_cb_analysis(p, df)
            except Exception as e:
                flash(f"Failed to undo elimination: {e}", "error")
                return _save_and_redirect(curr_idx)
            flash("Status reverted to Longlist.", "info")
            return _save_and_redirect(curr_idx)

        if action == "skip":
            next_idx = min(curr_idx + 1, len(df) - 1)
            return _save_and_redirect(next_idx)

        if action == "prev":
            prev_idx = max(0, curr_idx - 1)
            return _save_and_redirect(prev_idx)

        if action == "finish_labeling":
            try:
                # Persist any current edits to disk.
                io_csv.write_cb_analysis(p, df)
                flash(f"Draft saved to {Path(analysis_path).name}.", "success")
            except Exception as e:
                flash(f"Finish/save failed: {e}", "error")
            return redirect(url_for("crunchbase"))

    row_dict = df.iloc[curr_idx].to_dict() if 0 <= curr_idx < len(df) else {}

    return render_template(
        "crunchbase_analyze.html",
        analysis_path=str(p),
        row_idx=int(curr_idx),
        total_rows=int(len(df)),
        counts=counts,
        row=row_dict,
        columns=list(df.columns),
        progress={
            "index": (int(curr_idx) + 1) if len(df) > 0 else 0,
            "total": int(len(df)),
            "y": counts.get("y", 0),
            "n": counts.get("n", 0),
            "unlabeled": counts.get("unlabeled", 0),
        },
    )


@app.route("/news", methods=["GET", "POST"])
def news() -> ResponseReturnValue:
    columns, rows, message = [], [], ""
    today = datetime.now().date()
    default_from = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    default_to = today.strftime("%Y-%m-%d")
    form_query = '("Series A" OR "Series B" OR "Series C") AND (expansion OR international expansion OR global expansion OR expand internationally OR expand globally)'
    form_domains = "techcrunch.com, news.crunchbase.com, venturebeat.com, theinformation.com, sifted.eu, siliconangle.com, geekwire.com, reuters.com, bloomberg.com, ft.com, startupgrind.com, eu-startups.com, startups.co, pitchbook.com, dealroom.co, tech.eu, thenextweb.com, techinasia.com, vccafe.com, strictlyvc.com, axios.com, forbes.com, fastcompany.com, businessinsider.com, techpoint.africa, african.business, startupdaily.net, yourstory.com, e27.co, thesaasnews.com, prnewswire.com, businesswire.com, globenewswire.com, einpresswire.com, accesswire.com, newswire.com"
    form_language = "en"
    form_from_date = default_from
    form_to_date = default_to

    if request.method == "POST":
        action = request.form.get("action", "").strip()
        ts = datetime.now().strftime("%Y-%m-%d_%H%M")

        if action == "api_fetch":
            query = request.form.get("query", "").strip()
            domains = request.form.get("domains", "").strip()
            language = request.form.get("language", "en").strip()
            from_date = request.form.get("from_date", "").strip()
            to_date = request.form.get("to_date", "").strip()
            form_query = query or form_query
            form_domains = domains or form_domains
            form_language = language or form_language
            form_from_date = from_date or form_from_date
            form_to_date = to_date or form_to_date

            if not query:
                message = "Please provide a query before fetching."
            else:
                try:
                    articles = news_api.fetch_everything(
                        query=query,
                        from_date=from_date or None,
                        to_date=to_date or None,
                        language=language or "en",
                        domains=domains or None,
                    )
                except Exception as exc:
                    message = f"News API fetch failed: {exc}"
                else:
                    df_raw = news_ingest.articles_to_df(articles)
                    if df_raw.empty:
                        message = "No articles returned for that query."
                    else:
                        raw_path = config.NEWS_FETCH_DIR / f"NEWS_{ts}_raw.csv"
                        df_raw.to_csv(raw_path, sep=";", index=False, encoding="utf-8")
                        df_norm = news_ingest.normalize_articles(
                            df_raw,
                            query=query,
                            domains=domains,
                            language=language,
                        )
                        staged_path = config.NEWS_NORMALIZED_DIR / f"NEWS_{ts}_normalized.csv"
                        df_norm.to_csv(staged_path, sep=";", index=False, encoding="utf-8")
                        columns, rows = df_norm.columns.tolist(), df_norm.head(20).to_dict("records")
                        message = f"Fetched {len(df_norm)} articles. Previewing normalized results."

        elif action == "manual":
            file = request.files.get("file")
            if file and file.filename:
                raw_path = config.NEWS_FETCH_DIR / f"NEWS_{ts}_raw.csv"
                file.save(raw_path)
                df_raw = news_ingest.load_manual_csv(raw_path)
                if df_raw is None or df_raw.empty:
                    message = "Uploaded file is empty or unreadable."
                else:
                    df_norm = news_ingest.normalize_articles(df_raw)
                    staged_path = config.NEWS_NORMALIZED_DIR / f"NEWS_{ts}_normalized.csv"
                    df_norm.to_csv(staged_path, sep=";", index=False, encoding="utf-8")
                    columns, rows = df_norm.columns.tolist(), df_norm.head(20).to_dict("records")
                    message = f"Loaded {len(df_norm)} records from upload."
            else:
                message = "Please choose a CSV file to upload."

    return render_template(
        "news.html",
        columns=columns,
        rows=rows,
        message=message,
        form_query=form_query,
        form_domains=form_domains,
        form_language=form_language,
        form_from_date=form_from_date,
        form_to_date=form_to_date,
    )


@app.route("/other", methods=["GET"])
def other() -> ResponseReturnValue:
    return render_template("other.html")


def _split_full_name(full_name: str) -> tuple[str, str]:
    """
    Split 'Full Name' into (first, last).
    - Handles 'Last, First' as well.
    - Uses last token as last name for 'First Middle Last'.
    """
    s = (full_name or "").strip()
    if not s:
        return "", ""
    if "," in s:
        last, first = [p.strip() for p in s.split(",", 1)]
        return first, last
    parts = s.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


@app.route("/leadspicker/drafts", methods=["GET", "POST"])
def leadspicker_draft_messages() -> ResponseReturnValue:
    path_str = session.get("lp_drafts_path")
    if not path_str:
        flash("Start drafting from Leadspicker > Draft messages.", "warning")
        return redirect(url_for("leadspicker"))
    p = Path(path_str)
    if not p.exists():
        flash("Draft file not found. Start a new drafting session.", "error")
        return redirect(url_for("leadspicker"))

    df = io_csv.read_csv(p)
    if "remove_from_drafting" not in df.columns:
        df["remove_from_drafting"] = ""

    df = df.copy()
    df["remove_from_drafting"] = df["remove_from_drafting"].astype(str).str.lower()
    df_view = df[df["remove_from_drafting"] != "y"]

    total_all = len(df)
    view_indices = list(df_view.index)
    view_total = len(view_indices)
    curr_idx = int(session.get("lp_drafts_row_idx", 0))
    curr_idx = max(0, min(curr_idx, max(0, view_total - 1)))

    if view_total == 0:
        flash("No records selected for drafting. Use Edit drafted records to re-enable rows.", "warning")
        return redirect(url_for("leadspicker_draft_edit"))

    if request.method == "POST":
        action = request.form.get("action", "")
        if curr_idx < 0 or curr_idx >= len(view_indices):
            curr_idx = 0
        actual_idx = view_indices[curr_idx]

        # Auto-save current draft text on any action to avoid losing work
        if action:
            msg = request.form.get("message_text")
            if msg is not None:
                if "message_draft" not in df.columns:
                    df["message_draft"] = ""
                df.at[actual_idx, "message_draft"] = msg.strip()
                io_csv.write_csv_atomic(p, df)

        if action == "save_draft":
            remove_flag = str(df.at[actual_idx, "remove_from_drafting"]).lower() == "y"
            if remove_flag:
                df = df.drop(index=actual_idx).reset_index(drop=True)
                io_csv.write_csv_atomic(p, df)
                # Recompute remaining view length for pointer
                remaining = len(df[df["remove_from_drafting"].astype(str).str.lower() != "y"])
                session["lp_drafts_row_idx"] = max(0, min(curr_idx, max(0, remaining - 1)))
                flash("Draft saved and record removed from drafting batch.", "info")
            else:
                io_csv.write_csv_atomic(p, df)
                session["lp_drafts_row_idx"] = int(curr_idx)
                flash("Draft saved.", "info")
            return redirect(url_for("leadspicker_draft_messages"))

        if action == "prev":
            session["lp_drafts_row_idx"] = max(0, curr_idx - 1)
            return redirect(url_for("leadspicker_draft_messages"))

        if action == "next":
            session["lp_drafts_row_idx"] = min(curr_idx + 1, max(0, view_total - 1))
            return redirect(url_for("leadspicker_draft_messages"))

        if action == "finish_drafting":
            # Drop rows marked for removal before concluding drafting
            if "remove_from_drafting" in df.columns:
                df = df[df["remove_from_drafting"].astype(str).str.lower() != "y"].reset_index(drop=True)
                io_csv.write_csv_atomic(p, df)
            flash(f"Drafts saved to {p.name}", "success")
            # If this drafted file lives under the Czech pipeline folder,
            # return to the Czech Leadspicker menu; otherwise go to general.
            try:
                base_cz = config.LP_CZECHIA_DRAFTED_DIR
                if _is_under(base_cz, p):
                    return redirect(url_for("leadspicker_czech"))
            except Exception:
                pass
            return redirect(url_for("leadspicker"))

        # --- NEW: Enrich-style editing on drafts ---
        if action == "open_enrich":
            session["lp_drafts_enrich_open"] = True
            return redirect(url_for("leadspicker_draft_messages"))

        if action == "close_enrich":
            session["lp_drafts_enrich_open"] = False
            return redirect(url_for("leadspicker_draft_messages"))

        if action == "save_enrich":
            # read form fields
            full_name = (request.form.get("enrich_full_name") or "").strip()
            first_input = (request.form.get("enrich_first_name") or "").strip()
            last_input  = (request.form.get("enrich_last_name") or "").strip()
            linkedin  = (request.form.get("enrich_linkedin") or "").strip()
            relation  = (request.form.get("enrich_relation") or "").strip()
            comp_name = (request.form.get("enrich_company_name") or "").strip()
            website   = (request.form.get("enrich_company_website") or "").strip()
            comp_li   = (request.form.get("enrich_company_linkedin") or "").strip()

            # determine first/last name
            if first_input or last_input:
                # if user provided explicit first/last, trust those
                first, last = first_input, last_input
            else:
                # otherwise derive from full name
                try:
                    first, last = _split_full_name(full_name)  # assumes helper exists (used in analyze)
                except NameError:
                    # fallback minimal split
                    parts = full_name.split()
                    first = " ".join(parts[:-1]) if len(parts) > 1 else (parts[0] if parts else "")
                    last = parts[-1] if len(parts) > 1 else ""

            # ensure columns
            for col in [
                "global_first_name", "global_last_name", "global_linkedin_url",
                "global_relation_to_the_company", "global_company_name_raw",
                "global_domain_norm", "global_company_linkedin",
            ]:
                if col not in df.columns:
                    df[col] = ""

            # save
            idx = actual_idx
            df.at[idx, "global_first_name"] = first
            df.at[idx, "global_last_name"] = last
            df.at[idx, "global_linkedin_url"] = linkedin
            df.at[idx, "global_relation_to_the_company"] = relation
            df.at[idx, "global_company_name_raw"] = comp_name
            df.at[idx, "global_domain_norm"] = website
            df.at[idx, "global_company_linkedin"] = comp_li

            io_csv.write_csv_atomic(p, df)
            session["lp_drafts_enrich_open"] = False
            flash("Lead details saved.", "success")
            return redirect(url_for("leadspicker_draft_messages"))

    row = df.loc[view_indices[curr_idx]].to_dict() if view_total > 0 else {}
    first = (row.get("global_first_name") or row.get("lp_lead_first_name") or "").strip()
    last = (row.get("global_last_name") or row.get("lp_lead_last_name") or "").strip()
    author_name = (f"{first} {last}").strip()
    author_url = (row.get("global_linkedin_url") or row.get("lp_contacted_lead_linkedin") or "").strip()
    company_name = (row.get("global_company_name_raw") or row.get("lp_company_name") or "").strip()
    company_url = (row.get("global_company_linkedin") or row.get("lp_company_linkedin") or "").strip()
    company_domain = (row.get("global_domain_norm") or row.get("lp_company_website") or "").strip()
    relation = (row.get("global_relation_to_the_company") or row.get("lp_relation_to_the_company") or "").strip()
    post_text = (row.get("lp_linkedin_post") or "").strip()
    post_url = (row.get("lp_base_post_url") or "").strip()
    message_text = (row.get("message_draft") or "").strip()

    global_first_only = (row.get("global_first_name") or "").strip()
    global_last_only = (row.get("global_last_name") or "").strip()
    lp_first_only = (row.get("lp_lead_first_name") or "").strip()
    lp_last_only = (row.get("lp_lead_last_name") or "").strip()
    global_company_name = (row.get("global_company_name_raw") or "").strip()
    lp_company_name = (row.get("lp_company_name") or "").strip()
    lp_relation_only = (row.get("lp_relation_to_the_company") or "").strip()
    author_lp_url = (row.get("lp_contacted_lead_linkedin") or "").strip()
    lp_company_url = (
        row.get("lp_company_linkedin_cleaned")
        or row.get("lp_company_linkedin")
        or ""
    ).strip()

    def _join_name(first: str, last: str) -> str:
        return " ".join(part for part in (first, last) if part).strip()

    def _normalize(value: str) -> str:
        return value.strip().lower()

    global_full_name = _join_name(global_first_only, global_last_only)
    lp_full_name = _join_name(lp_first_only, lp_last_only)

    author_statement = ""
    author_conflict = False
    if global_full_name or lp_full_name:
        if _normalize(global_full_name) == _normalize(lp_full_name):
            author_statement = "This is the author of the post"
        else:
            author_conflict = True

    company_statement = ""
    if global_company_name or lp_company_name:
        if _normalize(global_company_name) == _normalize(lp_company_name):
            company_statement = "The post is written by a person from the company"
        else:
            company_statement = "The post is written by an outsider to the company"

    enrich_open = bool(session.get("lp_drafts_enrich_open", False))
    enrich_full_name = (f"{first} {last}").strip()
    return render_template(
        "leadspicker/draft_messages.html",
        path_name=p.name,
        idx=curr_idx + 1,
        total=view_total,
        author_name=author_name,
        author_url=author_url,
        company_name=company_name,
        company_url=company_url,
        company_domain=company_domain,
        relation=relation,
        post_text=post_text,
        post_url=post_url,
        message_text=message_text,
        first_name=first or "there",
        enrich_open=enrich_open,
        enrich_full_name=enrich_full_name,
        enrich_first_name=first,
        enrich_last_name=last,
        enrich_linkedin=author_url,
        enrich_relation=relation,
        enrich_company_name=company_name,
        enrich_company_website=company_domain,
        enrich_company_linkedin=company_url,
        author_statement=author_statement,
        company_statement=company_statement,
        author_conflict=author_conflict,
        author_lp_full_name=lp_full_name or author_name or "this author",
        author_lp_url=author_lp_url,
        author_lp_relation=lp_relation_only or relation or "contact",
        author_lp_company=lp_company_name or company_name or "the company",
        author_lp_company_url=lp_company_url,
    )


@app.route("/leadspicker/drafts/edit", methods=["GET", "POST"])
def leadspicker_draft_edit() -> ResponseReturnValue:
    path_str = session.get("lp_drafts_path")
    if not path_str:
        flash("Start drafting from Leadspicker > Draft messages.", "warning")
        return redirect(url_for("leadspicker"))
    p = Path(path_str)
    if not p.exists():
        flash("Draft file not found. Start a new drafting session.", "error")
        return redirect(url_for("leadspicker"))

    df = io_csv.read_csv(p)
    if "remove_from_drafting" not in df.columns:
        df["remove_from_drafting"] = ""

    df = df.copy()
    df["remove_from_drafting"] = df["remove_from_drafting"].astype(str).str.lower()

    if request.method == "POST":
        checked_ids = set(request.form.getlist("remove_ids"))
        for idx in df.index:
            df.at[idx, "remove_from_drafting"] = "y" if str(idx) in checked_ids else ""
        io_csv.write_csv_atomic(p, df)
        if request.form.get("action") == "back":
            return redirect(url_for("leadspicker_draft_messages"))
        flash("Selections saved.", "info")
        return redirect(url_for("leadspicker_draft_edit"))

    def _join_name(first: str, last: str) -> str:
        return " ".join(part for part in (first, last) if part).strip()

    def _normalize(value: str) -> str:
        return value.strip().lower()

    rows = []
    for idx, row in df.iterrows():
        g_first = (row.get("global_first_name") or "").strip()
        g_last = (row.get("global_last_name") or "").strip()
        g_relation = (row.get("global_relation_to_the_company") or "").strip()
        g_company = (row.get("global_company_name_raw") or "").strip()

        lp_first = (row.get("lp_lead_first_name") or "").strip()
        lp_last = (row.get("lp_lead_last_name") or "").strip()
        lp_company = (row.get("lp_company_name") or "").strip()

        global_full = _join_name(g_first, g_last)
        lp_full = _join_name(lp_first, lp_last)

        is_author = _normalize(global_full) == _normalize(lp_full) if (global_full or lp_full) else False
        is_from_company = _normalize(g_company) == _normalize(lp_company) if (g_company or lp_company) else False

        rows.append(
            {
                "idx": idx,
                "name": global_full or "(missing name)",
                "position": g_relation or "–",
                "company": g_company or "(missing company)",
                "is_author": "Yes" if is_author else "No",
                "is_from_company": "Yes" if is_from_company else "No",
                "post_text": (row.get("lp_linkedin_post") or "").strip(),
                "checked": str(row.get("remove_from_drafting") or "").lower() == "y",
            }
        )

    return render_template(
        "leadspicker/draft_edit.html",
        path_name=p.name,
        rows=rows,
    )
