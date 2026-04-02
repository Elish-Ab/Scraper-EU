import requests

BASE = "http://localhost:8000"

resp = requests.get(f"{BASE}/get-job-links", params={
    "url": "https://jobs.ashbyhq.com/wayflyer",
    "days": 0,
    "use_checkpoint": "false",
    "save_progress": "false"
})
data = resp.json()
print(f"Full response: {data}")
print(f"Total jobs: {data['total']}")
print(f"Method: {data.get('method', 'N/A')}")
print(f"Note: {data.get('note', '')}")

for job in data.get('jobs', [])[:3]:
    print(f"  - {job['title']} | {job.get('published','no date')} | {job.get('location','')}")

if data.get('jobs'):
    job_url = data['jobs'][0]['url']
    detail = requests.get(f"{BASE}/get-job-details", params={"url": job_url})
    job = detail.json().get("job", {})
    print(f"\nDetail for: {job.get('title')}")
    print(f"  Location:    {job.get('location')}")
    print(f"  Type:        {job.get('type')}")
    print(f"  Workplace:   {job.get('workplace')}")
    print(f"  Department:  {job.get('department')}")
    print(f"  Description: {len(job.get('description',''))} chars")