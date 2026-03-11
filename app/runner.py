import json
import requests

def load_sources(filename):
    with open(filename) as f:
        return json.load(f)  # now just a plain list

def scrape_boards(boards, label):
    results = []
    failed = []

    for i, board in enumerate(boards, 1):
        url = board["url"]
        print(f"[{label}] [{i}/{len(boards)}] {url}")
        try:
            resp = requests.get(
                "http://localhost:8000/get-job-links",
                params={"url": url, "days": 5},
                timeout=120
            )
            data = resp.json()
            jobs = data.get("jobs", [])
            print(f"  ✅ {data.get('total', 0)} jobs via {data.get('method')}")
            results.extend(jobs)
        except Exception as e:
            print(f"  ❌ {e}")
            failed.append(url)

    return results, failed


# --- Load both files ---
workable_boards = load_sources("workable_boards.json")
lever_boards = load_sources("lever_boards.json")

# --- Scrape both ---
workable_jobs, workable_failed = scrape_boards(workable_boards, "WORKABLE")
lever_jobs, lever_failed = scrape_boards(lever_boards, "LEVER")

# --- Save results ---
all_jobs = workable_jobs + lever_jobs

with open("all_job_links.json", "w") as f:
    json.dump(all_jobs, f, indent=2)

all_failed = workable_failed + lever_failed
if all_failed:
    with open("failed_boards.json", "w") as f:
        json.dump(all_failed, f, indent=2)

# --- Summary ---
print(f"\n{'='*50}")
print(f"WORKABLE: {len(workable_jobs)} jobs from {len(workable_boards) - len(workable_failed)}/{len(workable_boards)} boards")
print(f"LEVER:    {len(lever_jobs)} jobs from {len(lever_boards) - len(lever_failed)}/{len(lever_boards)} boards")
print(f"TOTAL:    {len(all_jobs)} jobs")
if all_failed:
    print(f"FAILED:   {len(all_failed)} boards → see failed_boards.json")
print(f"Saved →   all_job_links.json")