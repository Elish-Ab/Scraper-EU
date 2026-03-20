import json
import requests
import os
from datetime import datetime, timezone

API_BASE = "https://scraper-eu-production.up.railway.app"
DAYS = 5
OUTPUT_FILE = "output/all_jobs.json"

os.makedirs("output", exist_ok=True)

def load_sources(filename):
    with open(filename) as f:
        return json.load(f)

def save_job(job: dict):
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            try:
                all_jobs = json.load(f)
            except:
                all_jobs = []
    else:
        all_jobs = []
    all_jobs.append(job)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(all_jobs, f, indent=2)

def scrape_board(board_url) -> list:
    print(f"  🔍 Links: {board_url}")
    try:
        resp = requests.get(
            f"{API_BASE}/get-job-links",
            params={"url": board_url, "days": DAYS, "use_checkpoint": "false"},
            timeout=300
        )
        jobs = resp.json().get("jobs", [])
        print(f"    ✓ {len(jobs)} jobs")
    except Exception as e:
        print(f"    ❌ {e}")
        return []

    results = []
    for i, job in enumerate(jobs, 1):
        job_url    = job.get("url") if isinstance(job, dict) else job
        posted     = job.get("posted", "")      # e.g. "Posted 2 days ago"
        published  = job.get("published", "")   # e.g. "2026-03-19"
        title      = job.get("title", "?")

        date_str = published or posted or "no date"
        print(f"    [{i}/{len(jobs)}] {title[:50]} | 📅 {date_str}")
        print(f"           {job_url}")

        try:
            detail_resp = requests.get(
                f"{API_BASE}/get-job-details",
                params={"url": job_url},
                timeout=300
            )
            detail = detail_resp.json().get("job") or {}
            merged = {**job, **detail} if isinstance(job, dict) else detail
        except Exception as e:
            print(f"      ❌ {e}")
            merged = job if isinstance(job, dict) else {"url": job_url}

        # Ensure published date is always set on the merged job
        if not merged.get("published") and published:
            merged["published"] = published
        if not merged.get("posted") and posted:
            merged["posted"] = posted

        # Add scrape timestamp
        merged["scraped_at"] = datetime.now(timezone.utc).isoformat()

        save_job(merged)
        results.append(merged)
        print(f"      💾 Saved ({len(results)} total so far)")

    return results

# Clear output file at start of run
with open(OUTPUT_FILE, "w") as f:
    json.dump([], f)

workable_boards = load_sources("workable_boards.json")
lever_boards    = load_sources("lever_boards.json")

all_jobs = []

print("\n=== WORKABLE ===")
for board in workable_boards:
    all_jobs.extend(scrape_board(board["url"]))

print("\n=== LEVER ===")
for board in lever_boards:
    all_jobs.extend(scrape_board(board["url"]))

print(f"\n✅ Total: {len(all_jobs)} jobs → {OUTPUT_FILE}")

# Summary by date
if all_jobs:
    by_date = {}
    for job in all_jobs:
        date = job.get("published") or job.get("posted") or "unknown"
        by_date[date] = by_date.get(date, 0) + 1
    print("\n📊 Jobs by date:")
    for date in sorted(by_date.keys(), reverse=True):
        print(f"   {date}: {by_date[date]} jobs")