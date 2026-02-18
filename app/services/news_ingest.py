from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd


NEWS_COLUMNS = [
    "news_title",
    "news_description",
    "news_content",
    "news_url",
    "news_source",
    "news_author",
    "news_published_at",
    "news_url_to_image",
    "news_query",
    "news_domains",
    "news_language",
    "news_fetched_at",
]


def _series_of_len(df: pd.DataFrame, fill: str = "") -> pd.Series:
    return pd.Series([fill] * len(df), index=df.index, dtype="string")


def _pick(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    for c in candidates:
        if c in df.columns:
            return df[c].astype(str)
    lower_map = {col.lower(): col for col in df.columns}
    for c in candidates:
        lc = c.lower()
        if lc in lower_map:
            return df[lower_map[lc]].astype(str)
    return _series_of_len(df)


def align_to_news_schema(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in NEWS_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    extras = [c for c in out.columns if c not in NEWS_COLUMNS]
    return out[NEWS_COLUMNS + extras]


def articles_to_df(articles: list[dict]) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for art in articles or []:
        source = art.get("source") or {}
        rows.append(
            {
                "title": art.get("title", ""),
                "description": art.get("description", ""),
                "content": art.get("content", ""),
                "url": art.get("url", ""),
                "source": source.get("name", ""),
                "author": art.get("author", ""),
                "publishedAt": art.get("publishedAt", ""),
                "urlToImage": art.get("urlToImage", ""),
            }
        )
    return pd.DataFrame(rows)


def normalize_articles(
    df: pd.DataFrame,
    query: str = "",
    domains: str = "",
    language: str = "en",
    fetched_at: str | None = None,
) -> pd.DataFrame:
    fetched_at = fetched_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    out = pd.DataFrame(index=df.index)
    out["news_title"] = _pick(df, ["news_title", "title", "headline"])
    out["news_description"] = _pick(df, ["news_description", "description", "summary"])
    out["news_content"] = _pick(df, ["news_content", "content", "body"])
    out["news_url"] = _pick(df, ["news_url", "url", "link"])
    out["news_source"] = _pick(df, ["news_source", "source", "source_name"])
    out["news_author"] = _pick(df, ["news_author", "author"])
    out["news_published_at"] = _pick(df, ["news_published_at", "publishedAt", "published_at", "date"])
    out["news_url_to_image"] = _pick(df, ["news_url_to_image", "urlToImage", "image"])
    out["news_query"] = query
    out["news_domains"] = domains
    out["news_language"] = language
    out["news_fetched_at"] = fetched_at
    return align_to_news_schema(out)


def load_manual_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    except Exception:
        return pd.DataFrame()
