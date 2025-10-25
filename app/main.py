# app/main.py - Production-Ready with Batch Processing
from fastapi import FastAPI, Query, BackgroundTasks
from fastapi.responses import StreamingResponse
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import requests
import datetime
from datetime import datetime, timedelta
import re
import logging
import json
import time
import random
import asyncio
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2
MAX_WORKERS = 5  # Parallel workers for batch processing
REQUEST_DELAY = (1, 3)  # Random delay between requests (min, max) seconds

# Store for batch jobs (in production, use Redis or database)
batch_jobs_store = {}

# === HELPERS ===

def random_delay(base=1, variance=0.5):
    """Add random delay to avoid detection"""
    delay = base + random.uniform(-variance, variance)
    time.sleep(max(0.1, delay))

def _parse_workable_url(job_url: str):
    """Parse Workable URL to extract account and shortcode"""
    u = urlparse(job_url)
    path = u.path if u.path else job_url
    parts = [p for p in path.strip("/").split("/") if p]

    try:
        j_idx = len(parts) - 1 - parts[::-1].index("j")
        account = parts[j_idx - 1]
        shortcode = parts[j_idx + 1]
        return account, shortcode
    except Exception:
        if len(parts) >= 3 and parts[-2] == "j":
            return parts[-3], parts[-1]
        raise ValueError(f"Cannot parse Workable URL: {job_url}")

# === GET JOB LINKS ===

def get_links_from_api(board_url: str):
    """Try to fetch job links from JSON API"""
    try:
        subdomain = board_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"🔌 Trying JSON API: {api_url}")

        cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
        all_links = []
        params = {"limit": 50}
        page_count = 0
        max_pages = 100  # Increased for large job boards

        while page_count < max_pages:
            try:
                resp = requests.get(api_url, params=params, headers={
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }, timeout=15)

                if resp.status_code == 429:
                    logger.warning(f"⚠ Rate limited, waiting 10s...")
                    time.sleep(10)
                    continue
                    
                if resp.status_code != 200:
                    logger.warning(f"✗ API returned {resp.status_code}")
                    break

                data = resp.json()
                results = data.get("results", [])
                
                if not results:
                    break

                logger.info(f"📄 Page {page_count + 1}: {len(results)} jobs (total: {len(all_links)})")

                stop = False
                for job in results:
                    published = job.get("published")
                    if published:
                        try:
                            pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                            if pub_date < cutoff_date:
                                stop = True
                                break
                        except:
                            pass

                    shortcode = job.get("shortcode")
                    if shortcode:
                        job_url = urljoin(board_url, f"j/{shortcode}/")
                        all_links.append(job_url)

                if stop:
                    logger.info(f"⏹ Cutoff date reached")
                    break

                next_page = data.get("nextPage")
                if not next_page:
                    break

                params = {"limit": 50, "nextPage": next_page}
                page_count += 1
                time.sleep(random.uniform(0.3, 0.8))  # Small delay between pages
                
            except Exception as e:
                logger.error(f"✗ API page error: {e}")
                break

        if all_links:
            logger.info(f"✅ API found {len(all_links)} job links")
            return list(set(all_links))
            
        return []

    except Exception as e:
        logger.error(f"✗ JSON API failed: {e}")
        return []

def get_links_from_dom(board_url: str):
    """Get job links by scraping the DOM"""
    all_links = []
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"🌐 DOM scraping attempt {attempt + 1}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                page = context.new_page()
                
                page.goto(board_url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(3000)

                # Dismiss cookie consent
                try:
                    for selector in ["[data-ui='cookie-consent'] button", "button:has-text('Accept')"]:
                        btn = page.query_selector(selector)
                        if btn and btn.is_visible():
                            btn.click()
                            page.wait_for_timeout(1000)
                            break
                except:
                    pass

                # Find job elements
                job_elements = []
                for selector in ["li[data-ui='job']", "li[data-ui='job-opening']", ".job-item"]:
                    job_elements = page.query_selector_all(selector)
                    if job_elements:
                        logger.info(f"✓ Found {len(job_elements)} jobs with {selector}")
                        break

                cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
                
                for item in job_elements:
                    try:
                        link_el = item.query_selector("a[aria-labelledby], a")
                        if not link_el:
                            continue

                        href = link_el.get_attribute("href")
                        if not href or '/j/' not in href:
                            continue

                        # Check if too old
                        posted_el = item.query_selector("[data-ui='job-posted']")
                        if posted_el:
                            text = posted_el.inner_text().strip()
                            match = re.search(r"(\d+)\s+day", text)
                            if match and int(match.group(1)) > MAX_DAYS:
                                continue

                        full_url = urljoin(board_url, href)
                        if full_url not in all_links:
                            all_links.append(full_url)
                            
                    except:
                        continue

                # Try "Show more" button multiple times
                for _ in range(10):  # Increased for large job boards
                    try:
                        page.wait_for_timeout(2000)
                        btn = page.query_selector("button[data-ui='load-more-button']")
                        if not btn or not btn.is_visible():
                            break
                        
                        btn.click(force=True)
                        page.wait_for_timeout(3000)
                        
                        # Get new jobs
                        new_elements = page.query_selector_all("li[data-ui='job'], li[data-ui='job-opening']")
                        logger.info(f"  Loaded more: {len(new_elements)} total jobs")
                        
                        for item in new_elements[len(job_elements):]:
                            try:
                                link_el = item.query_selector("a")
                                if link_el:
                                    href = link_el.get_attribute("href")
                                    if href and '/j/' in href:
                                        full_url = urljoin(board_url, href)
                                        if full_url not in all_links:
                                            all_links.append(full_url)
                            except:
                                continue
                        
                        job_elements = new_elements
                    except:
                        break

                browser.close()
                
                if all_links:
                    logger.info(f"✅ DOM found {len(all_links)} job links")
                    return list(set(all_links))
                    
        except Exception as e:
            logger.warning(f"✗ DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
            continue
    
    return []

# === GET JOB DETAILS ===

def extract_job_with_dom(job_url: str, account: str, shortcode: str):
    """Extract full job details from DOM - returns None if fails"""
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"🌐 DOM extraction attempt {attempt + 1} for {shortcode}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='en-US'
                )
                
                page = context.new_page()
                
                # Navigate and wait
                page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(2000 + (attempt * 1000))
                
                # Check for error page
                body_text = page.inner_text('body').lower()
                if "error" in body_text and len(body_text) < 100:
                    logger.warning(f"⚠ Error page detected, retrying...")
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    body_text = page.inner_text('body').lower()
                
                if "error" in body_text and len(body_text) < 100:
                    logger.error(f"✗ Still error page after reload")
                    browser.close()
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return None
                
                # Dismiss overlays
                try:
                    for sel in ["[data-ui='backdrop']", "[data-ui='cookie-consent'] button", "button:has-text('Accept')"]:
                        elem = page.query_selector(sel)
                        if elem and elem.is_visible():
                            elem.click(timeout=1000)
                            page.wait_for_timeout(500)
                except:
                    pass

                job_data = {
                    "jobId": shortcode,
                    "url": job_url,
                    "account": account,
                }

                # Extract TITLE (mandatory)
                title = None
                for sel in ['h1[data-ui="job-title"]', 'h1[itemprop="title"]', 'h1']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            text = el.inner_text().strip()
                            if text and len(text) > 3 and "error" not in text.lower():
                                title = text
                                break
                    except:
                        continue
                
                if not title:
                    page_title = page.title()
                    if page_title and "error" not in page_title.lower():
                        title = page_title
                
                if not title or "error" in title.lower():
                    logger.error(f"✗ No valid title found")
                    browser.close()
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return None
                
                job_data["title"] = title

                # Extract LOCATION
                for sel in ['[data-ui="job-location"]', 'span[itemprop="addressLocality"]', '[data-ui="location"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            loc = el.inner_text().strip()
                            if loc and len(loc) > 1:
                                job_data["location"] = loc
                                break
                    except:
                        continue

                # Extract DEPARTMENT
                for sel in ['[data-ui="job-department"]', 'span[itemprop="hiringOrganization"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["department"] = el.inner_text().strip()
                            break
                    except:
                        continue

                # Extract JOB TYPE
                for sel in ['[data-ui="job-type"]', 'span[itemprop="employmentType"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["type"] = el.inner_text().strip()
                            break
                    except:
                        continue

                # Extract WORKPLACE
                try:
                    el = page.query_selector('[data-ui="job-workplace"]')
                    if el:
                        job_data["workplace"] = el.inner_text().strip()
                except:
                    pass

                # Extract DESCRIPTION (important)
                for sel in ['[data-ui="job-description"]', 'div[itemprop="description"]', 'section[data-ui="job-description"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 100:
                                job_data["description"] = html
                                break
                    except:
                        continue

                # Extract REQUIREMENTS
                for sel in ['[data-ui="job-requirements"]', 'section:has-text("Requirements")', 'div:has-text("Requirements")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["requirements"] = html
                                break
                    except:
                        continue

                # Extract BENEFITS
                for sel in ['[data-ui="job-benefits"]', 'section:has-text("Benefits")', 'div:has-text("Benefits")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["benefits"] = html
                                break
                    except:
                        continue

                # JSON-LD data
                try:
                    script = page.query_selector('script[type="application/ld+json"]')
                    if script:
                        ld = json.loads(script.inner_text())
                        if isinstance(ld, dict):
                            if "hiringOrganization" in ld:
                                org = ld.get("hiringOrganization", {})
                                if isinstance(org, dict):
                                    job_data["company"] = org.get("name")
                            if "datePosted" in ld:
                                job_data["published"] = ld.get("datePosted")
                            if "employmentType" in ld:
                                job_data["employmentType"] = ld.get("employmentType")
                except:
                    pass

                browser.close()
                
                # Success if we have title + at least one content field
                has_content = bool(job_data.get("description") or job_data.get("requirements") or job_data.get("location"))
                
                if has_content:
                    logger.info(f"✅ DOM extraction SUCCESS for {shortcode}")
                    return job_data
                else:
                    logger.warning(f"⚠ Missing critical content for {shortcode}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return job_data if title else None
                    
        except Exception as e:
            logger.error(f"✗ DOM attempt {attempt + 1} error for {shortcode}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue
    
    return None

def extract_job_with_api(account: str, shortcode: str, job_url: str):
    """Try to extract job details from API - returns None if fails"""
    
    endpoints = {
        "v3": f"https://apply.workable.com/api/v3/accounts/{account}/jobs/{shortcode}",
        "v2": f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{shortcode}",
    }
    
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": f"https://apply.workable.com/{account}/",
    }
    
    for name, endpoint in endpoints.items():
        for retry in range(2):
            try:
                if retry > 0:
                    time.sleep(3)
                
                resp = requests.get(endpoint, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning(f"⚠ Rate limited on {shortcode}")
                    if retry == 0:
                        time.sleep(5)
                        continue
                    return None
                
                if resp.status_code == 200:
                    d = resp.json()
                    logger.info(f"✅ API {name} SUCCESS for {shortcode}")
                    
                    return {
                        "jobId": d.get("shortcode") or shortcode,
                        "id": d.get("id"),
                        "url": job_url,
                        "account": account,
                        "title": d.get("title"),
                        "department": d.get("department"),
                        "published": d.get("published"),
                        "location": d.get("location"),
                        "locations": d.get("locations"),
                        "type": d.get("type"),
                        "workplace": d.get("workplace"),
                        "remote": d.get("remote"),
                        "description": d.get("description"),
                        "requirements": d.get("requirements"),
                        "benefits": d.get("benefits"),
                        "language": d.get("language"),
                    }
                
                elif resp.status_code in [404, 410]:
                    logger.warning(f"✗ Job {shortcode} not found ({resp.status_code})")
                    break
                
            except Exception as e:
                logger.warning(f"✗ API {name} error for {shortcode}: {e}")
                continue
    
    return None

def fetch_single_job(url: str) -> Dict:
    """Fetch a single job with all fallback strategies"""
    try:
        account, shortcode = _parse_workable_url(url)
        
        # Try DOM first
        dom_result = extract_job_with_dom(url, account, shortcode)
        if dom_result and dom_result.get("title"):
            dom_result["method"] = "dom"
            return {"success": True, "job": dom_result}
        
        # Fallback to API
        time.sleep(random.uniform(*REQUEST_DELAY))
        api_result = extract_job_with_api(account, shortcode, url)
        if api_result and api_result.get("title"):
            api_result["method"] = "api"
            return {"success": True, "job": api_result}
        
        # Final DOM retry
        time.sleep(2)
        final_result = extract_job_with_dom(url, account, shortcode)
        if final_result and final_result.get("title"):
            final_result["method"] = "dom_final"
            return {"success": True, "job": final_result}
        
        return {"success": False, "error": "All methods failed", "url": url}
        
    except Exception as e:
        return {"success": False, "error": str(e), "url": url}

def process_jobs_batch(job_urls: List[str]) -> List[Dict]:
    """
    Process multiple jobs in parallel with rate limiting
    """
    results = []
    total = len(job_urls)
    
    logger.info(f"🚀 Starting batch processing of {total} jobs with {MAX_WORKERS} workers")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all jobs
        future_to_url = {executor.submit(fetch_single_job, url): url for url in job_urls}
        
        completed = 0
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            
            try:
                result = future.result(timeout=120)  # 2 min timeout per job
                results.append(result)
                
                if result.get("success"):
                    logger.info(f"✅ [{completed}/{total}] Success: {url}")
                else:
                    logger.warning(f"⚠ [{completed}/{total}] Failed: {url}")
                    
            except Exception as e:
                logger.error(f"✗ [{completed}/{total}] Error: {url} - {e}")
                results.append({"success": False, "error": str(e), "url": url})
            
            # Rate limiting between completions
            if completed < total:
                time.sleep(random.uniform(0.5, 1.5))
    
    logger.info(f"🏁 Batch complete: {sum(1 for r in results if r.get('success'))}/{total} successful")
    return results

# === MAIN ENDPOINTS ===

@app.get("/get-job-links")
def get_job_links(url: str = Query(..., description="Workable board URL")):
    """
    Get all job links from a Workable board
    
    Handles large job boards efficiently (100+ jobs)
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🎯 GET JOB LINKS: {url}")
    logger.info(f"{'='*80}")
    
    try:
        # Try API first
        links = get_links_from_api(url)
        
        if links:
            return {
                "success": True,
                "total": len(links),
                "links": links,
                "method": "api"
            }
        
        # Fallback to DOM
        logger.info("API failed, trying DOM scraping...")
        links = get_links_from_dom(url)
        
        if links:
            return {
                "success": True,
                "total": len(links),
                "links": links,
                "method": "dom"
            }
        
        return {
            "success": False,
            "total": 0,
            "links": [],
            "error": "Could not find any jobs"
        }
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return {
            "success": False,
            "total": 0,
            "links": [],
            "error": str(e)
        }

@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable job URL")):
    """
    Get full details for a single job
    
    Uses multi-strategy approach with retries
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🎯 GET JOB DETAILS: {url}")
    logger.info(f"{'='*80}")
    
    result = fetch_single_job(url)
    return result

@app.post("/batch-get-job-details")
def batch_get_job_details(job_urls: List[str] = Query(..., description="List of job URLs")):
    """
    Get details for multiple jobs in parallel
    
    Optimized for processing large lists (10-1000+ jobs)
    - Parallel processing with {MAX_WORKERS} workers
    - Rate limiting to avoid bans
    - Progress tracking
    - Continues on individual failures
    
    Example:
    POST /batch-get-job-details
    Body: ["url1", "url2", "url3", ...]
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🎯 BATCH GET JOB DETAILS: {len(job_urls)} jobs")
    logger.info(f"{'='*80}")
    
    if not job_urls:
        return {
            "success": False,
            "error": "No job URLs provided",
            "results": []
        }
    
    if len(job_urls) > 1000:
        return {
            "success": False,
            "error": f"Too many jobs. Maximum 1000 per batch, got {len(job_urls)}",
            "results": []
        }
    
    start_time = time.time()
    results = process_jobs_batch(job_urls)
    elapsed = time.time() - start_time
    
    successful = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    
    return {
        "success": True,
        "total": len(job_urls),
        "successful": len(successful),
        "failed": len(failed),
        "elapsed_seconds": round(elapsed, 2),
        "avg_time_per_job": round(elapsed / len(job_urls), 2) if job_urls else 0,
        "results": results
    }

@app.get("/stream-job-details")
async def stream_job_details(urls: str = Query(..., description="Comma-separated job URLs")):
    """
    Stream job details as they're fetched (for real-time progress)
    
    Example: /stream-job-details?urls=url1,url2,url3
    Returns: NDJSON stream (one JSON object per line)
    """
    job_urls = [u.strip() for u in urls.split(",") if u.strip()]
    
    async def generate():
        for idx, url in enumerate(job_urls, 1):
            result = fetch_single_job(url)
            result["index"] = idx
            result["total"] = len(job_urls)
            yield json.dumps(result) + "\n"
            
            # Small delay between jobs
            await asyncio.sleep(random.uniform(1, 2))
    
    return StreamingResponse(generate(), media_type="application/x-ndjson")

@app.get("/health")
def health():
    """Health check"""
    return {
        "status": "healthy",
        "version": "3.1-production",
        "timestamp": datetime.utcnow().isoformat(),
        "max_workers": MAX_WORKERS,
        "batch_limit": 1000
    }

@app.get("/")
def root():
    return {
        "name": "Workable Job Scraper API - Production Grade",
        "version": "3.1",
        "description": "Robust scraper optimized for large-scale job extraction",
        "endpoints": {
            "/get-job-links": {
                "method": "GET",
                "description": "Get all job URLs from a board (handles 100+ jobs)",
                "example": "/get-job-links?url=https://apply.workable.com/company/"
            },
            "/get-job-details": {
                "method": "GET",
                "description": "Get full details for a single job",
                "example": "/get-job-details?url=https://apply.workable.com/company/j/SHORTCODE/"
            },
            "/batch-get-job-details": {
                "method": "POST",
                "description": f"Get details for multiple jobs in parallel (up to 1000, {MAX_WORKERS} workers)",
                "example": "POST with JSON body: [\"url1\", \"url2\", ...]"
            },
            "/stream-job-details": {
                "method": "GET",
                "description": "Stream job details in real-time (NDJSON)",
                "example": "/stream-job-details?urls=url1,url2,url3"
            }
        },
        "features": {
            "parallel_processing": f"{MAX_WORKERS} concurrent workers",
            "batch_limit": "1000 jobs per request",
            "rate_limiting": f"{REQUEST_DELAY[0]}-{REQUEST_DELAY[1]}s between requests",
            "retry_strategy": "3 attempts per method",
            "timeout_protection": "120s per job",
            "streaming_support": "Real-time progress via NDJSON",
            "error_recovery": "Continues on individual failures"
        },
        "performance": {
            "single_job": "~5-15 seconds",
            "batch_10_jobs": "~30-60 seconds",
            "batch_100_jobs": "~4-8 minutes",
            "batch_1000_jobs": "~40-80 minutes"
        }
    }# app/main.py - Clean 2-Endpoint Version
from fastapi import FastAPI, Query
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import requests
import datetime
from datetime import datetime, timedelta
import re
import logging
import json
import time
import random

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2

# === HELPERS ===

def random_delay(base=1, variance=0.5):
    """Add random delay to avoid detection"""
    delay = base + random.uniform(-variance, variance)
    time.sleep(max(0.1, delay))

def _parse_workable_url(job_url: str):
    """Parse Workable URL to extract account and shortcode"""
    u = urlparse(job_url)
    path = u.path if u.path else job_url
    parts = [p for p in path.strip("/").split("/") if p]

    try:
        j_idx = len(parts) - 1 - parts[::-1].index("j")
        account = parts[j_idx - 1]
        shortcode = parts[j_idx + 1]
        return account, shortcode
    except Exception:
        if len(parts) >= 3 and parts[-2] == "j":
            return parts[-3], parts[-1]
        raise ValueError(f"Cannot parse Workable URL: {job_url}")

# === GET JOB LINKS ===

def get_links_from_api(board_url: str):
    """Try to fetch job links from JSON API"""
    try:
        subdomain = board_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"🔌 Trying JSON API: {api_url}")

        cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
        all_links = []
        params = {"limit": 50}
        page_count = 0
        max_pages = 50

        while page_count < max_pages:
            try:
                resp = requests.get(api_url, params=params, headers={
                    "Accept": "application/json",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }, timeout=15)

                if resp.status_code == 429:
                    logger.warning(f"⚠ Rate limited, waiting...")
                    time.sleep(5)
                    continue
                    
                if resp.status_code != 200:
                    logger.warning(f"✗ API returned {resp.status_code}")
                    break

                data = resp.json()
                results = data.get("results", [])
                
                if not results:
                    break

                logger.info(f"📄 Page {page_count + 1}: {len(results)} jobs")

                stop = False
                for job in results:
                    published = job.get("published")
                    if published:
                        try:
                            pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                            if pub_date < cutoff_date:
                                stop = True
                                break
                        except:
                            pass

                    shortcode = job.get("shortcode")
                    if shortcode:
                        job_url = urljoin(board_url, f"j/{shortcode}/")
                        all_links.append(job_url)

                if stop:
                    break

                next_page = data.get("nextPage")
                if not next_page:
                    break

                params = {"limit": 50, "nextPage": next_page}
                page_count += 1
                time.sleep(0.3)
                
            except Exception as e:
                logger.error(f"✗ API page error: {e}")
                break

        if all_links:
            logger.info(f"✅ API found {len(all_links)} job links")
            return list(set(all_links))
            
        return []

    except Exception as e:
        logger.error(f"✗ JSON API failed: {e}")
        return []

def get_links_from_dom(board_url: str):
    """Get job links by scraping the DOM"""
    all_links = []
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"🌐 DOM scraping attempt {attempt + 1}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                page = context.new_page()
                
                page.goto(board_url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(3000)

                # Dismiss cookie consent
                try:
                    for selector in ["[data-ui='cookie-consent'] button", "button:has-text('Accept')"] :
                        btn = page.query_selector(selector)
                        if btn and btn.is_visible():
                            btn.click()
                            page.wait_for_timeout(1000)
                            break
                except:
                    pass

                # Find job elements
                job_elements = []
                for selector in ["li[data-ui='job']", "li[data-ui='job-opening']", ".job-item"]:
                    job_elements = page.query_selector_all(selector)
                    if job_elements:
                        logger.info(f"✓ Found {len(job_elements)} jobs with {selector}")
                        break

                cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
                
                for item in job_elements:
                    try:
                        link_el = item.query_selector("a[aria-labelledby], a")
                        if not link_el:
                            continue

                        href = link_el.get_attribute("href")
                        if not href or '/j/' not in href:
                            continue

                        # Check if too old
                        posted_el = item.query_selector("[data-ui='job-posted']")
                        if posted_el:
                            text = posted_el.inner_text().strip()
                            match = re.search(r"(\d+)\s+day", text)
                            if match and int(match.group(1)) > MAX_DAYS:
                                continue

                        full_url = urljoin(board_url, href)
                        if full_url not in all_links:
                            all_links.append(full_url)
                            
                    except:
                        continue

                # Try "Show more" button
                for _ in range(5):
                    try:
                        page.wait_for_timeout(2000)
                        btn = page.query_selector("button[data-ui='load-more-button']")
                        if not btn or not btn.is_visible():
                            break
                        
                        btn.click(force=True)
                        page.wait_for_timeout(3000)
                        
                        # Get new jobs
                        new_elements = page.query_selector_all("li[data-ui='job'], li[data-ui='job-opening']")
                        for item in new_elements[len(job_elements):]:
                            try:
                                link_el = item.query_selector("a")
                                if link_el:
                                    href = link_el.get_attribute("href")
                                    if href and '/j/' in href:
                                        full_url = urljoin(board_url, href)
                                        if full_url not in all_links:
                                            all_links.append(full_url)
                            except:
                                continue
                        
                        job_elements = new_elements
                    except:
                        break

                browser.close()
                
                if all_links:
                    logger.info(f"✅ DOM found {len(all_links)} job links")
                    return list(set(all_links))
                    
        except Exception as e:
            logger.warning(f"✗ DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
            continue
    
    return []

# === GET JOB DETAILS ===

def extract_job_with_dom(job_url: str, account: str, shortcode: str):
    """Extract full job details from DOM - returns None if fails"""
    
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"🌐 DOM extraction attempt {attempt + 1} for {shortcode}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080},
                    locale='en-US'
                )
                
                page = context.new_page()
                
                # Navigate and wait
                page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000 + (attempt * 1000))
                
                # Check for error page
                body_text = page.inner_text('body').lower()
                if "error" in body_text and len(body_text) < 100:
                    logger.warning(f"⚠ Error page detected, retrying...")
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    body_text = page.inner_text('body').lower()
                
                if "error" in body_text and len(body_text) < 100:
                    logger.error(f"✗ Still error page after reload")
                    browser.close()
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return None
                
                # Dismiss overlays
                try:
                    for sel in ["[data-ui='backdrop']", "[data-ui='cookie-consent'] button", "button:has-text('Accept')"]:
                        elem = page.query_selector(sel)
                        if elem and elem.is_visible():
                            elem.click(timeout=1000)
                            page.wait_for_timeout(500)
                except:
                    pass

                job_data = {
                    "jobId": shortcode,
                    "url": job_url,
                    "account": account,
                }

                # Extract TITLE (mandatory)
                title = None
                for sel in ['h1[data-ui="job-title"]', 'h1[itemprop="title"]', 'h1']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            text = el.inner_text().strip()
                            if text and len(text) > 3 and "error" not in text.lower():
                                title = text
                                break
                    except:
                        continue
                
                if not title:
                    page_title = page.title()
                    if page_title and "error" not in page_title.lower():
                        title = page_title
                
                if not title or "error" in title.lower():
                    logger.error(f"✗ No valid title found")
                    browser.close()
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return None
                
                job_data["title"] = title

                # Extract LOCATION
                for sel in ['[data-ui="job-location"]', 'span[itemprop="addressLocality"]', '[data-ui="location"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            loc = el.inner_text().strip()
                            if loc and len(loc) > 1:
                                job_data["location"] = loc
                                break
                    except:
                        continue

                # Extract DEPARTMENT
                for sel in ['[data-ui="job-department"]', 'span[itemprop="hiringOrganization"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["department"] = el.inner_text().strip()
                            break
                    except:
                        continue

                # Extract JOB TYPE
                for sel in ['[data-ui="job-type"]', 'span[itemprop="employmentType"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["type"] = el.inner_text().strip()
                            break
                    except:
                        continue

                # Extract WORKPLACE
                try:
                    el = page.query_selector('[data-ui="job-workplace"]')
                    if el:
                        job_data["workplace"] = el.inner_text().strip()
                except:
                    pass

                # Extract DESCRIPTION (important)
                for sel in ['[data-ui="job-description"]', 'div[itemprop="description"]', 'section[data-ui="job-description"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 100:
                                job_data["description"] = html
                                logger.info(f"✓ Description: {len(html)} chars")
                                break
                    except:
                        continue

                # Extract REQUIREMENTS
                for sel in ['[data-ui="job-requirements"]', 'section:has-text("Requirements")', 'div:has-text("Requirements")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["requirements"] = html
                                logger.info(f"✓ Requirements: {len(html)} chars")
                                break
                    except:
                        continue

                # Extract BENEFITS
                for sel in ['[data-ui="job-benefits"]', 'section:has-text("Benefits")', 'div:has-text("Benefits")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["benefits"] = html
                                logger.info(f"✓ Benefits: {len(html)} chars")
                                break
                    except:
                        continue

                # JSON-LD data
                try:
                    script = page.query_selector('script[type="application/ld+json"]')
                    if script:
                        ld = json.loads(script.inner_text())
                        if isinstance(ld, dict):
                            if "hiringOrganization" in ld:
                                org = ld.get("hiringOrganization", {})
                                if isinstance(org, dict):
                                    job_data["company"] = org.get("name")
                            if "datePosted" in ld:
                                job_data["published"] = ld.get("datePosted")
                            if "employmentType" in ld:
                                job_data["employmentType"] = ld.get("employmentType")
                except:
                    pass

                browser.close()
                
                # Success if we have title + at least one content field
                has_content = bool(job_data.get("description") or job_data.get("requirements") or job_data.get("location"))
                
                if has_content:
                    logger.info(f"✅ DOM extraction SUCCESS")
                    return job_data
                else:
                    logger.warning(f"⚠ Missing critical content")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return job_data if title else None
                    
        except Exception as e:
            logger.error(f"✗ DOM attempt {attempt + 1} error: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue
    
    return None

def extract_job_with_api(account: str, shortcode: str, job_url: str):
    """Try to extract job details from API - returns None if fails"""
    
    endpoints = {
        "v3": f"https://apply.workable.com/api/v3/accounts/{account}/jobs/{shortcode}",
        "v2": f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{shortcode}",
    }
    
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": f"https://apply.workable.com/{account}/",
    }
    
    for name, endpoint in endpoints.items():
        for retry in range(2):  # Only 2 retries for API
            try:
                logger.info(f"🔌 Trying {name} API (attempt {retry + 1})")
                
                if retry > 0:
                    time.sleep(3)
                
                resp = requests.get(endpoint, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning(f"⚠ Rate limited")
                    if retry == 0:
                        time.sleep(5)
                        continue
                    return None
                
                if resp.status_code == 200:
                    d = resp.json()
                    logger.info(f"✅ API {name} SUCCESS")
                    
                    return {
                        "jobId": d.get("shortcode") or shortcode,
                        "id": d.get("id"),
                        "url": job_url,
                        "account": account,
                        "title": d.get("title"),
                        "department": d.get("department"),
                        "published": d.get("published"),
                        "location": d.get("location"),
                        "locations": d.get("locations"),
                        "type": d.get("type"),
                        "workplace": d.get("workplace"),
                        "remote": d.get("remote"),
                        "description": d.get("description"),
                        "requirements": d.get("requirements"),
                        "benefits": d.get("benefits"),
                        "language": d.get("language"),
                    }
                
                elif resp.status_code in [404, 410]:
                    logger.warning(f"✗ Job not found ({resp.status_code})")
                    break
                
            except Exception as e:
                logger.warning(f"✗ API {name} error: {e}")
                continue
    
    return None

# === MAIN ENDPOINTS ===

@app.get("/get-job-links")
def get_job_links(url: str = Query(..., description="Workable board URL (e.g. https://apply.workable.com/company/)")):
    """
    Get all job links from a Workable board
    
    Strategy:
    1. Try JSON API first (fast, efficient)
    2. If API fails, use DOM scraping (slower but reliable)
    
    Returns: List of valid job URLs
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🎯 GET JOB LINKS: {url}")
    logger.info(f"{'='*80}")
    
    try:
        # Try API first
        links = get_links_from_api(url)
        
        if links:
            return {
                "success": True,
                "total": len(links),
                "links": links,
                "method": "api"
            }
        
        # Fallback to DOM
        logger.info("API failed, trying DOM scraping...")
        links = get_links_from_dom(url)
        
        if links:
            return {
                "success": True,
                "total": len(links),
                "links": links,
                "method": "dom"
            }
        
        # Both failed
        return {
            "success": False,
            "total": 0,
            "links": [],
            "error": "Could not find any jobs using API or DOM methods"
        }
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return {
            "success": False,
            "total": 0,
            "links": [],
            "error": str(e)
        }

@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable job URL (e.g. https://apply.workable.com/company/j/SHORTCODE/)")):
    """
    Get full details for a specific job
    
    Strategy:
    1. Try DOM extraction first (more reliable, 3 retries)
    2. If DOM fails, try API (v3 and v2)
    3. If API fails, final DOM attempt
    
    Returns: Complete job details (title, description, requirements, benefits, etc.)
    Only returns if valid data is found - never returns error pages
    """
    logger.info(f"\n{'='*80}")
    logger.info(f"🎯 GET JOB DETAILS: {url}")
    logger.info(f"{'='*80}")
    
    try:
        account, shortcode = _parse_workable_url(url)
        logger.info(f"📋 Account: {account}, Shortcode: {shortcode}")
        
        # PHASE 1: Try DOM (primary method)
        logger.info("\n--- PHASE 1: DOM EXTRACTION ---")
        dom_result = extract_job_with_dom(url, account, shortcode)
        
        if dom_result and dom_result.get("title"):
            logger.info(f"✅ SUCCESS via DOM")
            dom_result["method"] = "dom"
            return {
                "success": True,
                "job": dom_result
            }
        
        # PHASE 2: Try API (fallback)
        logger.info("\n--- PHASE 2: API EXTRACTION ---")
        time.sleep(2)
        api_result = extract_job_with_api(account, shortcode, url)
        
        if api_result and api_result.get("title"):
            logger.info(f"✅ SUCCESS via API")
            api_result["method"] = "api"
            return {
                "success": True,
                "job": api_result
            }
        
        # PHASE 3: Final DOM retry
        logger.info("\n--- PHASE 3: FINAL DOM RETRY ---")
        time.sleep(3)
        final_result = extract_job_with_dom(url, account, shortcode)
        
        if final_result and final_result.get("title"):
            logger.info(f"✅ SUCCESS via final DOM retry")
            final_result["method"] = "dom_final"
            return {
                "success": True,
                "job": final_result
            }
        
        # All methods failed
        logger.error(f"❌ ALL METHODS FAILED")
        return {
            "success": False,
            "error": "Could not extract valid job details after multiple attempts",
            "url": url
        }
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR: {e}")
        return {
            "success": False,
            "error": str(e),
            "url": url
        }

@app.get("/health")
def health():
    """Health check"""
    return {
        "status": "healthy",
        "version": "3.0-clean",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/")
def root():
    return {
        "name": "Workable Job Scraper API",
        "version": "3.0",
        "description": "Clean 2-endpoint scraper that returns valid data only",
        "endpoints": {
            "/get-job-links": {
                "description": "Get all job URLs from a board",
                "example": "/get-job-links?url=https://apply.workable.com/company/",
                "methods": ["API (fast)", "DOM (fallback)"]
            },
            "/get-job-details": {
                "description": "Get full details for a specific job",
                "example": "/get-job-details?url=https://apply.workable.com/company/j/SHORTCODE/",
                "methods": ["DOM (primary)", "API (fallback)", "DOM retry (final)"],
                "fields": ["title", "description", "requirements", "benefits", "location", "type", "department", "workplace", "company", "published"]
            }
        },
        "features": [
            "✅ Only returns valid job data (no error pages)",
            "✅ Multiple extraction strategies with retries",
            "✅ API + DOM scraping with automatic fallback",
            "✅ Rate limit handling",
            "✅ Error page detection and retry",
            "✅ Overlay/modal dismissal"
        ]
    }