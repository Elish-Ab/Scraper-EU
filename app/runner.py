import json
import requests
import os
from datetime import datetime

# ============================================================================
# CONFIG
# ============================================================================

API_BASE = "http://localhost:8000"
DAYS = 5

WORKABLE_FILE = "workable_boards.json"
LEVER_FILE    = "lever_boards.json"

# Output folders
LINKS_DIR   = "output/links"    # one file per board
DETAILS_DIR = "output/details"  # one file per job
SUMMARY_DIR = "output"

os.makedirs(LINKS_DIR, exist_ok=True)
os.makedirs(DETAILS_DIR, exist_ok=True)

# ============================================================================
# HELPERS
# ============================================================================

def safe_filename(url: str) -> str:
    """Convert a URL into a safe filename"""
    return url.replace("https://", "").replace("http://", "").replace("/", "_").strip("_")

def load_sources(filename: str) -> list:
    with open(filename) as f:
        return json.load(f)

def save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"    💾 Saved → {path}")

# ============================================================================
# STEP 1: GET JOB LINKS (one file per board)
# ============================================================================

def fetch_links_for_board(board_url: str) -> list:
    fname = safe_filename(board_url)
    out_path = os.path.join(LINKS_DIR, f"{fname}.json")

    # Skip if already done
    if os.path.exists(out_path):
        print(f"  ⏭️  Already scraped, loading from cache...")
        with open(out_path) as f:
            return json.load(f).get("jobs", [])

    try:
        resp = requests.get(
            f"{API_BASE}/get-job-links",
            params={"url": board_url, "days": DAYS},
            timeout=120
        )
        data = resp.json()
        jobs = data.get("jobs", [])

        # Save immediately
        save_json(out_path, {
            "board_url": board_url,
            "scraped_at": datetime.now().isoformat(),
            "total": len(jobs),
            "method": data.get("method"),
            "jobs": jobs
        })
        return jobs

    except Exception as e:
        print(f"  ❌ Links error: {e}")
        save_json(out_path, {"board_url": board_url, "error": str(e), "jobs": []})
        return []

# ============================================================================
# STEP 2: GET JOB DETAILS (one file per job)
# ============================================================================

def fetch_details_for_job(job_url: str) -> dict | None:
    fname = safe_filename(job_url)
    out_path = os.path.join(DETAILS_DIR, f"{fname}.json")

    # Skip if already done
    if os.path.exists(out_path):
        print(f"    ⏭️  Already scraped: {job_url}")
        return None

    try:
        resp = requests.get(
            f"{API_BASE}/get-job-details",
            params={"url": job_url},
            timeout=120
        )
        data = resp.json()

        # Save immediately
        save_json(out_path, {
            "job_url": job_url,
            "scraped_at": datetime.now().isoformat(),
            **data
        })
        return data.get("job")

    except Exception as e:
        print(f"    ❌ Details error: {e}")
        save_json(out_path, {"job_url": job_url, "error": str(e)})
        return None

# ============================================================================
# MAIN
# ============================================================================

def run(boards: list, label: str):
    all_jobs = []
    all_details = []

    print(f"\n{'='*60}")
    print(f"  {label}: {len(boards)} boards")
    print(f"{'='*60}")

    for i, board in enumerate(boards, 1):
        board_url = board["url"]
        print(f"\n[{label}] [{i}/{len(boards)}] {board_url}")

        # Step 1: get links
        jobs = fetch_links_for_board(board_url)
        print(f"  📋 {len(jobs)} job links found")
        all_jobs.extend(jobs)

        # Step 2: get details for each job immediately
        for j, job in enumerate(jobs, 1):
            job_url = job.get("url") if isinstance(job, dict) else job
            print(f"    [{j}/{len(jobs)}] {job_url}")
            detail = fetch_details_for_job(job_url)
            if detail:
                all_details.append(detail)

    return all_jobs, all_details


# --- Load boards ---
workable_boards = load_sources(WORKABLE_FILE)
lever_boards    = load_sources(LEVER_FILE)

# --- Run both ---
w_jobs, w_details = run(workable_boards, "WORKABLE")
l_jobs, l_details = run(lever_boards,    "LEVER")

# --- Final summary files ---
save_json(os.path.join(SUMMARY_DIR, "all_job_links.json"),   w_jobs + l_jobs)
save_json(os.path.join(SUMMARY_DIR, "all_job_details.json"), w_details + l_details)

print(f"\n{'='*60}")
print(f"WORKABLE: {len(w_jobs)} links | {len(w_details)} details")
print(f"LEVER:    {len(l_jobs)} links | {len(l_details)} details")
print(f"TOTAL:    {len(w_jobs+l_jobs)} links | {len(w_details+l_details)} details")
print(f"\nOutput structure:")
print(f"  output/links/      ← one JSON per board ({len(w_jobs+l_jobs)} files)")
print(f"  output/details/    ← one JSON per job")
print(f"  output/all_job_links.json")
print(f"  output/all_job_details.json")