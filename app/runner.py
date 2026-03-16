import json
import requests

API_BASE = "https://scraper-eu-production.up.railway.app"
DAYS = 5

def load_sources(filename):
    with open(filename) as f:
        return json.load(f)

def scrape_board(board_url):
    print(f"  🔍 Links: {board_url}")
    try:
        resp = requests.get(
            f"{API_BASE}/get-job-links",
            params={"url": board_url, "days": DAYS},
            timeout=120
        )
        jobs = resp.json().get("jobs", [])
        print(f"    ✓ {len(jobs)} jobs")
    except Exception as e:
        print(f"    ❌ {e}")
        return []

    results = []
    for i, job in enumerate(jobs, 1):
        job_url = job.get("url") if isinstance(job, dict) else job
        print(f"    [{i}/{len(jobs)}] {job_url}")
        try:
            detail_resp = requests.get(
                f"{API_BASE}/get-job-details",
                params={"url": job_url},
                timeout=120
            )
            detail = detail_resp.json().get("job") or {}
            # Merge board metadata + detail
            merged = {**job, **detail} if isinstance(job, dict) else detail
            results.append(merged)
        except Exception as e:
            print(f"      ❌ {e}")
            if isinstance(job, dict):
                results.append(job)

    return results

# Load boards
workable_boards = load_sources("workable_boards.json")
lever_boards    = load_sources("lever_boards.json")

all_jobs = []

print("\n=== WORKABLE ===")
for board in workable_boards:
    all_jobs.extend(scrape_board(board["url"]))

print("\n=== LEVER ===")
for board in lever_boards:
    all_jobs.extend(scrape_board(board["url"]))

# Single output file
with open("output/all_jobs.json", "w") as f:
    json.dump(all_jobs, f, indent=2)

print(f"\n✅ Total: {len(all_jobs)} jobs → output/all_jobs.json")