# app/services/airtable_api.py
"""
Thin Airtable helper focused on the Crunchbase Source table.
Implements the snippet provided by the user and wraps pyairtable usage.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from pyairtable import Api

from config import AIRTABLE_API_KEY

# Constants for the Crunchbase source base/table
BASE_ID = "appSXOLAKJX3Vjo3n"
TABLE_NAME = "Crunchbase Source"
TABLE_NAME_ENCODED = "Crunchbase%20Source"
BASE_URL = f"https://api.airtable.com/v0/{BASE_ID}"
TABLE_URL = f"{BASE_URL}/{TABLE_NAME_ENCODED}"

# Leadspicker → Airtable (General post) base/table
LP_BASE_ID = "appSXOLAKJX3Vjo3n"
LP_TABLE_NAME = "Leadspicker - general post"
LP_TABLE_NAME_CZECH = "Leadspicker - czehcia post"

# Airtable field mapping (Airtable field -> app column or computed via callable)
LP_GENERAL_POST_FIELD_MAP: dict[str, object] = {
    "Company Name": "global_company_name_raw",
    "First Name": "global_first_name",
    "Full Name": lambda r: f"{r.get('global_first_name','')} {r.get('global_last_name','')}".strip(),
    "Last Name": "global_last_name",
    "Created": "global_pushed_leadspicker_at",
    "leadspicker_id": "global_leadspicker_contact_id",
    "Contact LinkedIn profile": "global_linkedin_url",
    "Base post URL": "lp_base_post_url",
    "General message": "message_draft",
    "Base post": "lp_linkedin_post",
    "Company LinkedIn URL": "global_company_linkedin",
    "Relation to the company": "global_relation_to_the_company",
    "status": "global_status",
    "Company website": "global_domain_norm",
}

# Lazily instantiate the client so tests can stub AIRTABLE_API_KEY
_api: Optional[Api] = None


def _get_api() -> Api:
    global _api
    if _api is None:
        if not AIRTABLE_API_KEY:
            raise RuntimeError("AIRTABLE_API_KEY is not configured.")
        _api = Api(AIRTABLE_API_KEY)
    return _api


def get_table_ids(base_id: str = BASE_ID) -> Dict[str, str]:
    """
    Fetch table ids and names for the given base.

    Requires AIRTABLE_API_KEY to be a PAT with schema.bases:read scope.
    Returns: dict mapping table_name -> table_id.
    """
    if not AIRTABLE_API_KEY:
        raise RuntimeError("AIRTABLE_API_KEY is not configured.")
    resp = requests.get(
        f"https://api.airtable.com/v0/meta/bases/{base_id}/tables",
        headers={"Authorization": f"Bearer {AIRTABLE_API_KEY}"},
        timeout=30,
    )
    resp.raise_for_status()
    return {t["name"]: t["id"] for t in resp.json().get("tables", [])}


def fetch_records(
    table_name: str,
    view: Optional[str] = None,
    fields: Optional[list[str]] = None,
    max_records: Optional[int] = None,
    filter_value: Optional[str] = None,
    filter_field: str = "Status",
    filter_formula: Optional[str] = None,
) -> List[dict]:
    """
    Fetch records from Airtable via pyairtable.

    Args mirror the provided snippet.
    """
    table = _get_api().table(BASE_ID, table_name)
    formula = filter_formula
    if formula is not None and "{" not in formula:
        formula = f"{{{filter_field}}}='{formula}'"
    if filter_value is not None and filter_formula is None:
        formula = f"{{{filter_field}}}='{filter_value}'"
    return table.all(
        view=view,
        fields=fields,
        max_records=max_records,
        formula=formula,
    )


def records_to_df(records: List[dict], include_id: bool = True) -> pd.DataFrame:
    """
    Convert Airtable records to a pandas DataFrame.
    If a record has a 'Message draft' field shaped like {'state': ..., 'value': ..., 'isStale': ...},
    the field is normalized to just its 'value'.
    """
    rows: list[dict[str, Any]] = []
    for rec in records:
        row = dict(rec.get("fields", {}))
        md = row.get("Message draft")
        if isinstance(md, dict) and "value" in md:
            row["Message draft"] = md["value"]
        if include_id:
            row["_id"] = rec.get("id", "")
        rows.append(row)
    return pd.DataFrame(rows)


def fetch_crunchbase_source(
    status_filter: Optional[str] = None,
    view: Optional[str] = None,
    max_records: Optional[int] = None,
    contact_enriched_zero: bool = False,
) -> pd.DataFrame:
    """
    Convenience wrapper to fetch from the Crunchbase Source table.
    status_filter maps to {Status}='value' unless view/filter_formula overrides it.
    """
    clauses: list[str] = []
    if status_filter:
        clauses.append(f"{{Status}}='{status_filter}'")
    if contact_enriched_zero:
        clauses.append("{Contact enriched}=0")

    if len(clauses) == 0:
        formula = None
    elif len(clauses) == 1:
        formula = clauses[0]
    else:
        # Use AND for multiple filters
        formula = f"AND({', '.join(clauses)})"

    records = fetch_records(
        table_name=TABLE_NAME,
        view=view,
        max_records=max_records,
        filter_value=None if formula else status_filter,
        filter_field="Status",
        filter_formula=formula,
    )
    return records_to_df(records)


def update_crunchbase_records(df: pd.DataFrame, batch_size: int = 10) -> dict:
    """
    Update Crunchbase Source rows in Airtable from a DataFrame using batch_update.
    Mirrors the provided helper signature and logic.
    """
    if df is None or df.empty:
        return {"attempted": 0, "updated": 0, "errors": ["DataFrame is empty"]}
    if "_id" not in df.columns:
        return {"attempted": 0, "updated": 0, "errors": ["Missing id column: _id"]}

    bad_cols = [
        "Half year reminder (suggested)",
        "Message draft",
        "CB financials link",
        "CB people link",
        "Contact enriched",
        "CB news link",
        "Tag",
        "Tags",
        "Reviewed by Roman",
        "Number of Investors",
        "relevant",
        "learning_data",
    ]
    df = df.drop(columns=bad_cols, errors="ignore").copy()

    if "Message fin" in df.columns:
        df["Message fin"] = df["Message fin"].apply(lambda v: "" if v is None else str(v))

    df = df.where(pd.notna(df), "")

    table = _get_api().table(BASE_ID, TABLE_NAME)
    cols = [c for c in df.columns if c != "_id"]

    updates: list[dict] = []
    for _, row in df.iterrows():
        rec_id = str(row.get("_id") or "").strip()
        if not rec_id:
            continue
        updates.append({"id": rec_id, "fields": {k: row.get(k) for k in cols}})

    if not updates:
        return {"attempted": 0, "updated": 0, "errors": ["No valid records with IDs"]}

    errors: list[str] = []
    details: list[str] = []
    updated = 0
    for i in range(0, len(updates), batch_size):
        chunk = updates[i : i + batch_size]
        try:
            # typecast=True to allow new single-select options
            resp = table.batch_update(chunk, typecast=True)
            updated += len(resp or [])
            details.append(f"Batch {i // batch_size + 1}: updated {len(resp or [])} records")
        except Exception as e:
            err = f"Batch {i // batch_size + 1}: {e}"
            resp = getattr(e, "response", None)
            if resp is not None:
                status = getattr(resp, "status_code", None)
                text = getattr(resp, "text", "") or ""
                err += f" (HTTP {status} {text[:300]})"
                try:
                    j = resp.json()
                    err += f" JSON={j}"
                except Exception:
                    pass
            errors.append(err)

    return {"attempted": len(updates), "updated": updated, "errors": errors, "details": details}


def create_leadspicker_general_post_records(
    df: pd.DataFrame,
    field_map: Optional[dict[str, object]] = None,
    batch_size: int = 10,
) -> dict:
    """
    Create new Airtable records in the Leadspicker 'General post' table.

    field_map: Airtable field -> dataframe column name OR callable(row_dict)->value
    """
    if df is None or df.empty:
        return {"attempted": 0, "created": 0, "errors": ["DataFrame is empty"]}

    if field_map is None:
        field_map = LP_GENERAL_POST_FIELD_MAP

    table = _get_api().table(LP_BASE_ID, LP_TABLE_NAME)
    return _create_airtable_records(table, df, field_map, batch_size)


def create_leadspicker_czech_post_records(
    df: pd.DataFrame,
    field_map: Optional[dict[str, object]] = None,
    batch_size: int = 10,
) -> dict:
    """
    Create new Airtable records in the Leadspicker 'Czech-focused post' table.
    """
    if df is None or df.empty:
        return {"attempted": 0, "created": 0, "errors": ["DataFrame is empty"]}

    if field_map is None:
        field_map = LP_GENERAL_POST_FIELD_MAP

    table = _get_api().table(LP_BASE_ID, LP_TABLE_NAME_CZECH)
    return _create_airtable_records(table, df, field_map, batch_size)


def _create_airtable_records(
    table,
    df: pd.DataFrame,
    field_map: dict[str, object],
    batch_size: int,
) -> dict:
    records: list[dict] = []
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        fields: dict[str, str] = {}
        for at_field, spec in field_map.items():
            val: object = ""
            if callable(spec):
                val = spec(row_dict)
            elif isinstance(spec, str) and spec in df.columns:
                val = row_dict.get(spec)
            if pd.isna(val):
                val = ""
            fields[at_field] = "" if val is None else str(val)
        if fields:
            records.append(fields)

    if not records:
        return {"attempted": 0, "created": 0, "errors": ["No rows produced any mapped fields"]}

    created = 0
    errors: list[str] = []
    details: list[str] = []
    for i in range(0, len(records), batch_size):
        chunk = records[i : i + batch_size]
        try:
            resp = table.batch_create(chunk, typecast=True)
            created += len(resp or [])
            details.append(f"Batch {i // batch_size + 1}: created {len(resp or [])} records")
        except Exception as e:
            err = f"Batch {i // batch_size + 1}: {e}"
            resp = getattr(e, "response", None)
            if resp is not None:
                status = getattr(resp, "status_code", None)
                text = getattr(resp, "text", "") or ""
                err += f" (HTTP {status} {text[:300]})"
                try:
                    j = resp.json()
                    err += f" JSON={j}"
                except Exception:
                    pass
            errors.append(err)

    return {"attempted": len(records), "created": created, "errors": errors, "details": details}
