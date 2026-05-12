import hashlib
import json
import os
import re
import threading
from datetime import datetime, timedelta

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from jobspy import scrape_jobs

app = FastAPI()
CACHE_FILE = "/tmp/jobs_cache.json"
CACHE_MAX_AGE_HOURS = 12  # кеш старше этого — перезаписываем полностью
CACHE_MAX_ITEMS = 500      # максимум записей в кеше

INDEED_TARGETS = [
    {"country": "cyprus",      "location": "Limassol"},
    {"country": "malta",       "location": "Malta"},
    {"country": "netherlands", "location": "Amsterdam"},
    {"country": "spain",       "location": "Barcelona"},
    {"country": "portugal",    "location": "Lisbon"},
    {"country": "germany",     "location": "Germany"},
]

SEARCH_TERMS = [
    "PHP Laravel developer",
    "Laravel Symfony backend engineer",
]

GOOGLE_QUERIES = [
    "Laravel developer Cyprus relocation",
    "PHP backend developer Malta",
    "Laravel Symfony developer Amsterdam",
    "PHP developer Barcelona relocation",
]

KEEP_COLS = [
    "site", "job_url", "title", "company", "location",
    "job_type", "date_posted", "min_amount", "max_amount",
    "currency", "interval", "description", "content_hash",
]


def make_hash(row) -> str:
    title = re.sub(r'[^a-z0-9]', '', (row.get('title') or '').lower())
    company = re.sub(r'[^a-z0-9]', '', (row.get('company') or '').lower())
    return hashlib.md5(f"{title}|{company}".encode()).hexdigest()


def scrape_linkedin() -> pd.DataFrame:
    dfs = []
    for term in SEARCH_TERMS:
        try:
            df = scrape_jobs(
                site_name=["linkedin"],
                search_term=term,
                location="Cyprus",
                results_wanted=50,
                hours_old=168,
                linkedin_fetch_description=True,
                description_format="markdown",
                verbose=1,
            )
            dfs.append(df)
        except Exception as e:
            print(f"[LinkedIn:{term}] {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def scrape_indeed_all() -> pd.DataFrame:
    dfs = []
    for target in INDEED_TARGETS:
        for term in SEARCH_TERMS:
            try:
                df = scrape_jobs(
                    site_name=["indeed"],
                    search_term=term,
                    location=target["location"],
                    country_indeed=target["country"],
                    results_wanted=30,
                    hours_old=168,
                    description_format="markdown",
                    verbose=1,
                )
                dfs.append(df)
            except Exception as e:
                print(f"[Indeed:{target['country']}:{term}] {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def scrape_google() -> pd.DataFrame:
    dfs = []
    for q in GOOGLE_QUERIES:
        try:
            df = scrape_jobs(
                site_name=["google"],
                google_search_term=q,
                results_wanted=20,
                description_format="markdown",
                verbose=1,
            )
            dfs.append(df)
        except Exception as e:
            print(f"[Google:{q}] {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def filter_relevant(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    pattern = "|".join([
        "php", "laravel", "symfony", "backend", "full.?stack",
        "software engineer", "web developer",
    ])
    return df[df["title"].str.lower().str.contains(pattern, na=False)].reset_index(drop=True)


def clean_value(v):
    """None вместо NaN/NaT для JSON-сериализации."""
    if v is None:
        return None
    if isinstance(v, float) and (v != v):  # NaN check
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    return v


def merge_with_cache(new_records: list[dict]) -> list[dict]:
    """
    Мёрджим новые записи со старым кешем:
    - дедуплицируем по content_hash
    - выкидываем записи старше CACHE_MAX_AGE_HOURS
    - обрезаем до CACHE_MAX_ITEMS
    """
    existing = []
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                data = json.load(f)
                cached_at_str = data.get("cached_at")
                existing = data.get("jobs", [])

                # если кеш слишком старый — не берём старые записи, только новые
                if cached_at_str:
                    cached_at = datetime.fromisoformat(cached_at_str)
                    if datetime.utcnow() - cached_at > timedelta(hours=CACHE_MAX_AGE_HOURS):
                        existing = []
        except Exception as e:
            print(f"[Cache] Failed to read cache: {e}")
            existing = []

    # индексируем существующие по hash для быстрого поиска
    existing_by_hash = {r["content_hash"]: r for r in existing if r.get("content_hash")}

    # добавляем новые, не перезаписывая уже существующие
    for record in new_records:
        h = record.get("content_hash")
        if h and h not in existing_by_hash:
            existing_by_hash[h] = record

    merged = list(existing_by_hash.values())

    # сортируем по дате (свежие сверху) и обрезаем
    merged.sort(key=lambda r: r.get("date_posted") or "", reverse=True)
    merged = merged[:CACHE_MAX_ITEMS]

    return merged


def run_scrape():
    print("[Scheduler] Starting scrape...")
    frames = []
    for fn in [scrape_linkedin, scrape_indeed_all, scrape_google]:
        try:
            frames.append(fn())
        except Exception as e:
            print(f"[Scheduler] {fn.__name__} failed: {e}")

    if not frames:
        print("[Scheduler] All scrapers failed, cache not updated")
        return

    combined = pd.concat(frames, ignore_index=True)

    # фильтрация по title
    combined = filter_relevant(combined)

    # оставляем нужные колонки
    existing_cols = [c for c in KEEP_COLS if c in combined.columns]
    combined = combined[existing_cols]

    # дедупликация внутри текущего прогона
    combined["content_hash"] = combined.apply(
        lambda row: make_hash(row.to_dict()), axis=1
    )
    combined = combined.drop_duplicates(subset=["content_hash"], keep="first")
    combined = combined.drop_duplicates(subset=["job_url"], keep="first")

    # сериализуем в list[dict], заменяя NaN → None
    new_records = []
    for record in combined.to_dict("records"):
        new_records.append({k: clean_value(v) for k, v in record.items()})

    # мёрджим с кешем
    merged = merge_with_cache(new_records)

    # сохраняем
    cache_data = {
        "cached_at": datetime.utcnow().isoformat(),
        "count": len(merged),
        "jobs": merged,
    }
    with open(CACHE_FILE, "w") as f:
        json.dump(cache_data, f, default=str, ensure_ascii=False)

    print(f"[Scheduler] Done. {len(new_records)} new scraped, {len(merged)} total in cache.")


scheduler = BackgroundScheduler()
scheduler.add_job(run_scrape, "interval", hours=6, id="scrape")
scheduler.start()


@app.on_event("startup")
def startup():
    threading.Thread(target=run_scrape, daemon=True).start()


@app.get("/")
def index():
    if not os.path.exists(CACHE_FILE):
        return JSONResponse(
            content={"status": "warming_up", "message": "First scrape in progress, check back in 2-3 min"},
            status_code=202,
        )
    with open(CACHE_FILE) as f:
        data = json.load(f)
    return JSONResponse(content=data)


@app.get("/refresh")
def refresh():
    threading.Thread(target=run_scrape, daemon=True).start()
    return {"status": "started"}