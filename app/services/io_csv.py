# app/services/io_csv.py
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import List

import pandas as pd

import config

# CSV defaults
SEP = ";"
ENCODING = "utf-8"

def _ensure_parent_dir(p: Path) -> None:
    Path(p).parent.mkdir(parents=True, exist_ok=True)

def _to_str_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    # make sure everything is str; preserve empty strings
    return df.astype(str).where(pd.notna(df), "")

def read_csv(path: Path) -> pd.DataFrame:
    path = Path(path)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(
        path,
        sep=SEP,
        dtype=str,
        encoding=ENCODING,
        keep_default_na=False,  # do not convert empty strings to NaN
        na_filter=False,
        on_bad_lines="skip",
    )

def write_csv_atomic(path: Path, df: pd.DataFrame) -> None:
    path = Path(path)
    _ensure_parent_dir(path)
    df = _to_str_df(df)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(
        tmp,
        sep=SEP,
        index=False,
        encoding=ENCODING,
        quoting=csv.QUOTE_MINIMAL,
        lineterminator="\n",
    )
    os.replace(tmp, path)

# ----- Label normalization ----------------------------------------------------

def normalize_relevant_value(val: str) -> str:
    if val is None:
        return ""
    raw = str(val).strip()
    s = raw.lower()
    if s in {"y", "yes", "true", "1"}:
        return "y"
    if s in {"n", "no", "false", "0"}:
        return "n"
    # Allow special marker for Compliance Checkpoint
    if s == "cc":
        return "CC"
    return ""

def normalize_yesno_value(val: str) -> str:
    # generic yes/no normalizer for other columns like learning_data
    return normalize_relevant_value(val)

def normalize_relevant_column(df: pd.DataFrame, col: str = "relevant") -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    if col not in df.columns:
        df[col] = ""
        return df
    df[col] = df[col].map(normalize_relevant_value)
    return df

def normalize_yesno_column(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    if col not in df.columns:
        df[col] = ""
        return df
    df[col] = df[col].map(normalize_yesno_value)
    return df

def ensure_analysis_has_relevant(df: pd.DataFrame) -> pd.DataFrame:
    # Back-compat wrapper; keep for older call sites
    return ensure_analysis_has_columns(df)

def ensure_analysis_has_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure analysis CSV has both 'relevant' and 'learning_data' columns.
    """
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()
    if "relevant" not in df.columns:
        df["relevant"] = ""
    if "learning_data" not in df.columns:
        df["learning_data"] = ""
    return df

# ----- Minimal schema checks --------------------------------------------------

def assert_key_present(df: pd.DataFrame, key: str = "lp_base_post_url") -> None:
    if key not in df.columns:
        raise ValueError(f"Required key column '{key}' is missing in DataFrame/CSV.")

# ----- LP memory helpers ------------------------------------------------------

def read_lp_labeling_memory() -> pd.DataFrame:
    df = read_csv(config.LP_LABELING_MEMORY_FILE)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    return _to_str_df(df)

def write_lp_labeling_memory(df: pd.DataFrame) -> None:
    df = _to_str_df(df)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    write_csv_atomic(config.LP_LABELING_MEMORY_FILE, df)

# ----- LP Czechia labeling memory helpers ------------------------------------

def read_lp_czechia_labeling_memory() -> pd.DataFrame:
    """
    Labeling memory for the Czech-focused Leadspicker pipeline.
    Mirrors read_lp_labeling_memory but uses LP_CZECHIA_LABELING_MEMORY_FILE.
    """
    df = read_csv(config.LP_CZECHIA_LABELING_MEMORY_FILE)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    return _to_str_df(df)

def write_lp_czechia_labeling_memory(df: pd.DataFrame) -> None:
    """
    Write helper for the Czech-focused Leadspicker labeling memory file.
    """
    df = _to_str_df(df)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    write_csv_atomic(config.LP_CZECHIA_LABELING_MEMORY_FILE, df)

# ----- CB memory helpers -----------------------------------------------------

def read_cb_labeling_memory() -> pd.DataFrame:
    df = read_csv(config.CB_LABELING_MEMORY_FILE)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    return _to_str_df(df)

def write_cb_labeling_memory(df: pd.DataFrame) -> None:
    df = _to_str_df(df)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    write_csv_atomic(config.CB_LABELING_MEMORY_FILE, df)

def append_to_lp_memory_idempotent(df_new: pd.DataFrame, key: str = "lp_base_post_url") -> dict:
    """
    Append df_new rows to lp_labeling_memory.csv if their key is not already present.
    - Keeps all columns (union of existing and new).
    - Drops rows with empty key.
    Returns stats dict: {"existing": int, "added": int, "skipped_empty_key": int, "final_total": int}
    """
    df_new = _to_str_df(df_new)
    if df_new is None or df_new.empty:
        mem = read_lp_labeling_memory()
        return {"existing": len(mem), "added": 0, "skipped_empty_key": 0, "final_total": len(mem)}

    assert_key_present(df_new, key)

    # Normalize label
    df_new = normalize_relevant_column(df_new, "relevant")

    # Drop rows with empty key
    df_new["_key"] = df_new[key].astype(str).str.strip()
    df_new_valid = df_new[df_new["_key"] != ""].drop(columns=["_key"])
    skipped_empty_key = len(df_new) - len(df_new_valid)

    mem = read_lp_labeling_memory()
    if mem.empty:
        # Union columns and write
        all_cols = list(dict.fromkeys(list(df_new_valid.columns)))
        df_out = df_new_valid.reindex(columns=all_cols, fill_value="")
        write_lp_labeling_memory(df_out)
        return {"existing": 0, "added": len(df_out), "skipped_empty_key": skipped_empty_key, "final_total": len(df_out)}

    assert_key_present(mem, key)

    # Build set of existing keys
    existing_keys = set(mem[key].astype(str).str.strip().tolist())

    # Filter new rows whose key not in memory
    df_add = df_new_valid[~df_new_valid[key].astype(str).str.strip().isin(existing_keys)]

    # Union columns (preserve order: existing first, then any new columns appended)
    all_cols = list(mem.columns)
    for c in df_add.columns:
        if c not in all_cols:
            all_cols.append(c)
    mem_u = mem.reindex(columns=all_cols, fill_value="")
    add_u = df_add.reindex(columns=all_cols, fill_value="")

    df_out = pd.concat([mem_u, add_u], ignore_index=True)
    write_lp_labeling_memory(df_out)

    return {
        "existing": len(mem),
        "added": len(add_u),
        "skipped_empty_key": skipped_empty_key,
        "final_total": len(df_out),
    }

# ----- LP analyses helpers ----------------------------------------------------

def list_lp_analysis_files(limit: int = 50) -> List[Path]:
    p = Path(config.LP_ANALYSES_DIR)
    if not p.exists():
        return []
    files = [f for f in p.glob("*.csv") if f.is_file()]
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files[:limit]

def read_lp_analysis(path: Path) -> pd.DataFrame:
    df = read_csv(path)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    df = ensure_analysis_has_columns(df)
    return _to_str_df(df)

def write_lp_analysis(path: Path, df: pd.DataFrame) -> None:
    df = _to_str_df(df)
    df = ensure_analysis_has_columns(df)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    write_csv_atomic(Path(path), df)

# ----- CB analyses helpers ----------------------------------------------------

def list_cb_analysis_files(limit: int = 50) -> List[Path]:
    p = Path(config.CB_ANALYSES_DIR)
    if not p.exists():
        return []
    files = [f for f in p.glob("*.csv") if f.is_file()]
    files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
    return files[:limit]

def read_cb_analysis(path: Path) -> pd.DataFrame:
    df = read_csv(path)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    df = ensure_analysis_has_columns(df)
    return _to_str_df(df)

def write_cb_analysis(path: Path, df: pd.DataFrame) -> None:
    df = _to_str_df(df)
    df = ensure_analysis_has_columns(df)
    df = normalize_relevant_column(df, "relevant")
    df = normalize_yesno_column(df, "learning_data")
    write_csv_atomic(Path(path), df)

# ----- Back-compat: legacy names (if referenced elsewhere) --------------------

# Some older code may import ANALYSES_DIR from config; that still works via alias.
# Keep read_master/write_master or other helpers above intact.
