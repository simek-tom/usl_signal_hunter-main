from __future__ import annotations

from typing import Any, Optional

import requests

from config import NEWS_API_KEY


BASE_URL = "https://newsapi.org/v2/everything"


def fetch_everything(
    query: str,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    language: str = "en",
    sort_by: str = "publishedAt",
    domains: Optional[str] = None,
    page_size: int = 100,
    page: int = 1,
) -> list[dict[str, Any]]:
    if not NEWS_API_KEY:
        raise RuntimeError("NEWS_API_KEY is not configured.")

    params = {
        "q": query,
        "language": language,
        "sortBy": sort_by,
        "pageSize": page_size,
        "page": page,
    }
    if from_date:
        params["from"] = from_date
    if to_date:
        params["to"] = to_date
    if domains:
        params["domains"] = domains

    resp = requests.get(
        BASE_URL,
        params=params,
        headers={"Authorization": NEWS_API_KEY},
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json() or {}
    return payload.get("articles", [])
