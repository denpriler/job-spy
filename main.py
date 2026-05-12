import json
import os
import threading

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from jobspy import scrape_jobs

app = FastAPI()
CACHE_FILE = "/tmp/jobs_cache.json"

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


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["job_url"], keep="first")
    df = df.drop_duplicates(subset=["title", "company"], keep="first")
    return df.reset_index(drop=True)


def filter_relevant(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    pattern = "|".join([
        "php", "laravel", "symfony", "backend", "full.?stack",
        "software engineer", "web developer",
    ])
    return df[df["title"].str.lower().str.contains(pattern, na=False)].reset_index(drop=True)


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
    combined = deduplicate(combined)
    combined = filter_relevant(combined)

    keep_cols = [
        "site", "job_url", "title", "company", "location",
        "job_type", "date_posted", "min_amount", "max_amount",
        "currency", "interval", "description",
    ]
    existing = [c for c in keep_cols if c in combined.columns]
    result = combined[existing].to_dict("records")

    with open(CACHE_FILE, "w") as f:
        json.dump(result, f, default=str)

    print(f"[Scheduler] Done. {len(result)} jobs cached.")


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
        return json.load(f)


@app.get("/refresh")
def refresh():
    threading.Thread(target=run_scrape, daemon=True).start()
    return {"status": "started"}