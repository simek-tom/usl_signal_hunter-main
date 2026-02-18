# app/services/leadspicker_api.py
import requests
import pandas as pd
from bs4 import BeautifulSoup
from config import LEADSPICKER_API_KEY

# -------------------- CSRF + session --------------------

def get_session_and_csrf():
    """
    Establish a new HTTP session and retrieve the CSRF token from LP API docs.
    Returns: (session or None, csrf_token or None, debug_message)
    Tries multiple extraction methods: body[data-csrf-token], meta[name=csrf-token],
    hidden inputs, and common cookie names.
    """
    session = requests.Session()
    # A real UA often avoids bot protection
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })

    endpoints = [
        "https://app.leadspicker.com/app/sb/api/docs",   # your original
        "https://app.leadspicker.com/app/sb/api/docs/",  # trailing slash
    ]

    last_status = None
    last_url = None
    last_reason = None
    csrf_token = None

    for url in endpoints:
        try:
            resp = session.get(url, allow_redirects=True, timeout=15)
        except Exception as e:
            last_status = "EXC"
            last_reason = str(e)
            continue

        last_status = resp.status_code
        last_url = resp.url
        last_reason = getattr(resp, "reason", "")

        if resp.status_code != 200:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # A) body[data-csrf-token]
        body = soup.find("body")
        if body and body.get("data-csrf-token"):
            csrf_token = body.get("data-csrf-token")

        # B) <meta name="csrf-token" content="...">
        if not csrf_token:
            meta = soup.find("meta", attrs={"name": "csrf-token"})
            if meta and meta.get("content"):
                csrf_token = meta.get("content")

        # C) hidden input (common in Django-style forms)
        if not csrf_token:
            inp = soup.find("input", attrs={"name": "csrfmiddlewaretoken"})
            if inp and inp.get("value"):
                csrf_token = inp.get("value")

        # D) cookies that often store CSRF
        if not csrf_token:
            for cname in ["csrftoken", "CSRF-TOKEN", "XSRF-TOKEN", "csrf"]:
                if cname in resp.cookies:
                    csrf_token = resp.cookies.get(cname)
                    break
                if cname in session.cookies:
                    csrf_token = session.cookies.get(cname)
                    break

        if csrf_token:
            debug = f"CSRF fetched from {last_url} (status {last_status})."
            return session, csrf_token, debug

    debug = f"Failed CSRF fetch. Last status={last_status} reason={last_reason} url={last_url}"
    return None, None, debug

# -------------------- List projects --------------------

def get_project_ids(session: requests.Session, csrf_token: str):
    """
    Fetch project IDs and names.
    Returns: [{"id": 123, "name": "Project A"}, ...]  (empty list on failure)
    """
    url = "https://app.leadspicker.com/app/sb/api/projects"
    headers = {
        "accept": "application/json",
        "X-API-Key": LEADSPICKER_API_KEY,
        "X-CSRFToken": csrf_token,
    }
    resp = session.get(url, headers=headers)
    if resp.status_code == 200:
        data = resp.json()
        return [{"id": entry.get("id"), "name": entry.get("name")} for entry in data]

    print(f"Failed to retrieve project IDs, status={resp.status_code}")
    return []

# -------------------- Project people (with pagination) --------------------

def get_project_info(session: requests.Session, csrf_token: str, project_id, page_size: int = 50):
    """
    Fetch all 'people' items for a project, following pages until API returns 400 or no items.
    Returns: list of item dicts (possibly empty)
    """
    headers = {
        "accept": "application/json",
        "X-API-Key": LEADSPICKER_API_KEY,
        "X-CSRFToken": csrf_token,
    }

    items = []
    page = 1
    while True:
        url = (
            f"https://app.leadspicker.com/app/sb/api/projects/{project_id}"
            f"/people?page={page}&page_size={page_size}&order_by=-created&unselected_ids=%5B%5D&selected_ids=%5B%5D"
        )
        resp = session.get(url, headers=headers)

        # Stop when LP signals "no more" with 400
        if resp.status_code == 400:
            break

        if resp.status_code != 200:
            print(f"Project {project_id}: HTTP {resp.status_code} on page {page}")
            break

        data = resp.json()
        page_items = data.get("items", [])
        if not page_items:
            # No more items; stop
            break

        items.extend(page_items)
        page += 1

    return items

def process_project_info(session: requests.Session, csrf_token: str, project_id) -> pd.DataFrame:
    """
    Convenience wrapper: returns a DataFrame for all people in a project.
    """
    items = get_project_info(session, csrf_token, project_id)
    if not items:
        return pd.DataFrame()
    return pd.json_normalize(items)

from typing import Tuple
import pandas as pd

PERSONS_ENDPOINT = "https://app.leadspicker.com/app/sb/api/persons"

def _s(v) -> str:
    return ("" if v is None else str(v)).strip()

def build_person_payload(row: dict, project_id: int) -> dict:
    """
    Build JSON for POST /persons using top-level fields + custom_fields.

    CSV -> Leadspicker:
      - global_first_name              -> first_name
      - global_last_name               -> last_name
      - global_linkedin_url            -> linkedin
      - global_relation_to_the_company -> position
      - global_company_name_raw        -> company_name
      - global_domain_norm             -> company_website
      - global_company_linkedin        -> company_linkedin
      - lp_base_post_url               -> custom_fields.base_post_url
      - message_draft                  -> custom_fields['Message - desc']
      - lp_linkedin_post               -> custom_fields.linkedin_post
      - (future) email_subject         -> custom_fields.email_subject
    """
    def _s(v) -> str:
        return ("" if v is None else str(v)).strip()

    # Person
    first = _s(row.get("global_first_name") or row.get("lp_lead_first_name"))
    last = _s(row.get("global_last_name") or row.get("lp_lead_last_name"))
    linkedin = _s(row.get("global_linkedin_url") or row.get("lp_contacted_lead_linkedin"))
    position = _s(row.get("global_relation_to_the_company") or row.get("lp_relation_to_the_company"))

    # Company
    company_name = _s(row.get("global_company_name_raw") or row.get("lp_company_name"))
    company_website = _s(row.get("global_domain_norm") or row.get("lp_company_website"))
    company_linkedin = _s(row.get("global_company_linkedin") or row.get("lp_company_linkedin"))

    # Extras → custom_fields (must match contact_type names in project headers)
    base_post_url = _s(row.get("lp_base_post_url"))
    # Drafted message goes into \"Message - desc\" column
    message_desc = _s(row.get("message_draft") or row.get("message_desc"))
    linkedin_post = _s(row.get("lp_linkedin_post"))
    email_subject = _s(row.get("email_subject"))  # optional, for future

    custom_fields: dict[str, str] = {}
    if base_post_url:
        custom_fields["base_post_url"] = base_post_url
    if message_desc:
        custom_fields["Message - desc"] = message_desc
    if linkedin_post:
        custom_fields["linkedin_post"] = linkedin_post
    if email_subject:
        custom_fields["email_subject"] = email_subject

    return {
        "project_id": int(project_id),
        "data_source": "user_provided",
        "first_name": first,
        "last_name": last,
        "linkedin": linkedin,
        "position": position,
        "company_name": company_name,
        "company_website": company_website,
        "company_linkedin": company_linkedin,
        # optional top-levels if you have them
        "email": _s(row.get("email")),
        "country": _s(row.get("country")),
        "salesnav": _s(row.get("salesnav")),
        "custom_fields": custom_fields,
    }

def post_person(payload: dict) -> Tuple[int, str]:
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "x-api-key": LEADSPICKER_API_KEY,
    }
    resp = requests.post(PERSONS_ENDPOINT, json=payload, headers=headers, timeout=20)
    return resp.status_code, resp.text

def push_drafted_dataframe(df: pd.DataFrame, project_id: int, limit: int | None = None) -> dict:
    if df is None or df.empty:
        return {"total": 0, "sent": 0, "ok": 0, "fail": 0, "errors": []}

    sent = ok = fail = 0
    errors: list[str] = []
    results: list[dict[str, object]] = []
    n = len(df) if limit is None else min(limit, len(df))

    for i in range(n):
        row = df.iloc[i].to_dict()
        payload = build_person_payload(row, project_id)
        row_result = {
            "index": i,
            "ok": False,
            "status": None,
            "response": "",
            "error": "",
        }
        try:
            status, text = post_person(payload)
            sent += 1
            row_result["status"] = status
            row_result["response"] = text[:200] if text else ""
            if 200 <= status < 300:
                ok += 1
                row_result["ok"] = True
            else:
                fail += 1
                err_msg = f"row {i+1}: HTTP {status} {text[:200]}"
                row_result["error"] = text[:200] if text else ""
                errors.append(err_msg)
        except Exception as e:
            fail += 1
            err_msg = f"row {i+1}: EXC {e}"
            row_result["error"] = str(e)
            errors.append(err_msg)
        results.append(row_result)

    return {
        "total": n,
        "sent": sent,
        "ok": ok,
        "fail": fail,
        "errors": errors[:5],
        "results": results,
    }
