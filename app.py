import csv
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from jobspy import scrape_jobs
from apscheduler.schedulers.background import BackgroundScheduler
import json, os

app = FastAPI()
CACHE_FILE = "/tmp/jobs_cache.json"


# --- scraper functions (без изменений) ---

def scrape_linkedin() -> pd.DataFrame:
    dfs = []
    for term in ["PHP Laravel developer", "Laravel Symfony backend engineer"]:
        df = scrape_jobs(
            site_name=["linkedin"], search_term=term, location="Cyprus",
            results_wanted=25, hours_old=72, job_type="fulltime",
            linkedin_fetch_description=True, description_format="markdown", verbose=1,
        )
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def scrape_indeed_all() -> pd.DataFrame:
    targets = [
        {"country": "cyprus",      "location": "Limassol"},
        {"country": "malta",       "location": "Malta"},
        {"country": "netherlands", "location": "Amsterdam"},
        {"country": "spain",       "location": "Barcelona"},
        {"country": "portugal",    "location": "Lisbon"},
        {"country": "germany",     "location": "Germany"},
    ]
    dfs = []
    for target in targets:
        for term in ["PHP Laravel developer", "Laravel Symfony backend engineer"]:
            try:
                df = scrape_jobs(
                    site_name=["indeed"], search_term=term,
                    location=target["location"], country_indeed=target["country"],
                    results_wanted=20, hours_old=72,
                    description_format="markdown", verbose=1,
                )
                dfs.append(df)
            except Exception as e:
                print(f"[Indeed:{target['country']}] {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def scrape_google() -> pd.DataFrame:
    queries = [
        "PHP Laravel developer jobs Cyprus OR Malta OR Netherlands relocation visa sponsorship",
        "Symfony backend engineer jobs Limassol OR Amsterdam OR Barcelona relocation",
    ]
    dfs = []
    for q in queries:
        try:
            df = scrape_jobs(
                site_name=["google"], google_search_term=q,
                results_wanted=20, description_format="markdown", verbose=1,
            )
            dfs.append(df)
        except Exception as e:
            print(f"[Google] {e}")
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
    pattern = "|".join(["php", "laravel", "symfony", "backend", "full stack", "fullstack"])
    return df[df["title"].str.lower().str.contains(pattern, na=False)].reset_index(drop=True)


# --- фоновый скрейпинг ---

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
    # Первый прогон при старте контейнера (в фоне, не блокирует старт)
    import threading
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
    """Ручной запуск скрейпинга (не ждёт завершения)."""
    import threading
    threading.Thread(target=run_scrape, daemon=True).start()
    return {"status": "started"}