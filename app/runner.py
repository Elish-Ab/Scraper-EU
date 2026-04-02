import json
import requests
import os
import sys
from datetime import datetime, timezone

# API_BASE = "https://scraper-eu-production.up.railway.app"
API_BASE = "http://localhost:8000"  # for local testing
OUTPUT_FILE = "output/all_jobs.json"

os.makedirs("output", exist_ok=True)

# ── Date range args ────────────────────────────────────────────────
# Usage:
#   python runner.py                          → last 5 days (default)
#   python runner.py --from 2026-03-18 --to 2026-03-23
#   python runner.py --days 7

args = sys.argv[1:]

def get_arg(name):
    if name in args:
        idx = args.index(name)
        if idx + 1 < len(args):
            return args[idx + 1]
    return None

from_str = get_arg("--from")
to_str   = get_arg("--to")
days_str = get_arg("--days")

if from_str:
    since_dt = datetime.fromisoformat(from_str).replace(tzinfo=timezone.utc)
    until_dt = datetime.fromisoformat(to_str).replace(tzinfo=timezone.utc) if to_str else datetime.now(timezone.utc)
    # Calculate days from today back to --from date
    DAYS = (datetime.now(timezone.utc) - since_dt).days + 1
    print(f"📅 Date range: {from_str} → {to_str or 'today'} ({DAYS} days)")
elif days_str:
    DAYS    = int(days_str)
    since_dt = None
    until_dt = None
    print(f"📅 Last {DAYS} days")
else:
    DAYS     = 2
    since_dt = None
    until_dt = None
    print(f"📅 Default: last {DAYS} days")

# ──────────────────────────────────────────────────────────────────

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

def in_date_range(job: dict) -> bool:
    """Check if job falls within the requested date range."""
    if not since_dt and not until_dt:
        return True
    pub = job.get("published") or job.get("posted", "")
    if not pub:
        return True  # no date → include

    # Extract ISO date from pub string
    try:
        # Handle "2026-03-19" or "2026-03-19T00:00:00.000Z"
        pub_date = datetime.fromisoformat(pub[:10]).replace(tzinfo=timezone.utc)
    except:
        return True

    if since_dt and pub_date < since_dt:
        return False
    if until_dt and pub_date > until_dt:
        return False
    return True

def scrape_board(board_url) -> list:
    print(f"  🔍 Links: {board_url}")
    try:
        resp = requests.get(
            f"{API_BASE}/get-job-links",
            params={"url": board_url, "days": DAYS, "use_checkpoint": "false"},
            timeout=300
        )
        jobs = resp.json().get("jobs", [])
        print(f"    ✓ {len(jobs)} jobs from API")
    except Exception as e:
        print(f"    ❌ {e}")
        return []

    # Client-side filter for the upper bound (--to date)
    if until_dt or since_dt:
        jobs = [j for j in jobs if in_date_range(j)]
        print(f"    ✓ {len(jobs)} jobs after date filter")

    results = []
    for i, job in enumerate(jobs, 1):
        job_url   = job.get("url") if isinstance(job, dict) else job
        published = job.get("published", "")
        posted    = job.get("posted", "")
        title     = job.get("title", "?")

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

        if not merged.get("published") and published:
            merged["published"] = published
        if not merged.get("posted") and posted:
            merged["posted"] = posted

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
        date = (job.get("published") or job.get("posted") or "unknown")[:10]
        by_date[date] = by_date.get(date, 0) + 1
    print("\n📊 Jobs by date:")
    for date in sorted(by_date.keys(), reverse=True):
        print(f"   {date}: {by_date[date]} jobs")
        
        
# runner.py — add ashby boards
ashby_boards = load_sources("ashby_boards.json")

print("\n=== ASHBY ===")
for board in ashby_boards:
    all_jobs.extend(scrape_board(board["url"]))