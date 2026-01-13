#  app/main.py
from fastapi import FastAPI, Query #type: ignore
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout #type: ignore
import requests
from datetime import datetime, timedelta, timezone
import re
import logging
import json
import time
import random

from app.lever_scraper import extract_job_with_lever_api, get_links_from_lever_api, is_lever_url

app = FastAPI()

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

def get_links_from_api(board_url: str, days: int = 5):
    """
    Get jobs from LAST {days} DAYS using POST/GET methods
    - Deduplicates by title
    - Merges locations/countries
    - Uses Cloudflare cookies
    """
    try:
        # Clean URL - remove hash fragments
        clean_url = board_url.split('#')[0].rstrip('/')
        subdomain = clean_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"🔌 API: Fetching jobs from last {days} days...")
        logger.info(f"   Subdomain: {subdomain}")

        # === 1. Get Cloudflare cookies ===
        cookies = {}
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
                )
                page = context.new_page()
                page.goto(clean_url, wait_until="networkidle", timeout=30000)
                page.wait_for_timeout(3000)
                
                # Dismiss cookie consent
                try:
                    page.click("text=Accept", timeout=3000)
                except:
                    pass
                
                for c in context.cookies():
                    if c["name"] in ["cf_clearance", "__cf_bm", "wmc"]:
                        cookies[c["name"]] = c["value"]
                browser.close()
        except Exception as e:
            logger.warning(f"Cookie extraction failed: {e}")

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Origin": "https://apply.workable.com",
            "Referer": board_url,
        }
        
        if cookies:
            headers["Cookie"] = "; ".join([f"{k}={v}" for k, v in cookies.items()])
            logger.info(f"   Using cookies: {list(cookies.keys())}")

        # === 2. Try POST then GET ===
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        all_jobs = []
        
        for method in ["POST", "GET"]:
            try:
                logger.info(f"   Trying {method} method...")
                
                next_page = None
                page_num = 0
                all_jobs = []
                pagination_incomplete = False

                while page_num < 100:
                    page_num += 1
                    
                    if method == "POST":
                        payload = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
                        if next_page:
                            # For POST, nextPage goes at root level
                            payload = {
                                "query": "",
                                "location": [],
                                "department": [],
                                "worktype": [],
                                "remote": [],
                                "nextPage": next_page
                            }
                        
                        logger.debug(f"   POST payload: {json.dumps(payload)[:100]}")
                        resp = requests.post(api_url, json=payload, headers=headers, timeout=20)
                    else:
                        params = {"limit": 50}
                        if next_page:
                            params["nextPage"] = next_page
                        resp = requests.get(api_url, params=params, headers=headers, timeout=20)

                    if resp.status_code == 429:
                        logger.warning(f"   Rate limited, waiting...")
                        time.sleep(10)
                        continue
                        
                    if resp.status_code == 400:
                        logger.warning(f"   {method} pagination failed with 400 (nextPage token issue)")
                        pagination_incomplete = True
                        break
                        
                    if resp.status_code != 200:
                        logger.warning(f"   {method} failed: {resp.status_code}")
                        break

                    data = resp.json()
                    results = data.get("results", [])
                    
                    if not results:
                        logger.info(f"   No results on page {page_num}")
                        break
                    
                    logger.info(f"   Page {page_num}: {len(results)} jobs")

                    # Collect all jobs, track if page has recent jobs
                    page_has_recent_jobs = False
                    for job in results:
                        pub_str = job.get("published")
                        is_recent = True
                        
                        if pub_str:
                            if pub_str.endswith("Z"):
                                pub_str = pub_str[:-1] + "+00:00"
                            
                            try:
                                pub_date = datetime.fromisoformat(pub_str)
                                if pub_date >= cutoff_date:
                                    page_has_recent_jobs = True
                                else:
                                    is_recent = False
                            except:
                                pass

                        # Collect all jobs regardless of date (we'll filter later)
                        all_jobs.append(job)
                    
                    # Only stop if entire page has NO recent jobs
                    if not page_has_recent_jobs and page_num > 1:
                        logger.info(f"   Page {page_num} has no recent jobs, stopping pagination")
                        break

                    next_page = data.get("nextPage")
                    if not next_page:
                        break

                    time.sleep(0.5)

                if all_jobs and not pagination_incomplete:
                    logger.info(f"✅ {method} fetched {len(all_jobs)} jobs successfully")
                    break
                elif all_jobs and pagination_incomplete:
                    logger.info(f"⚠ {method} fetched {len(all_jobs)} jobs but pagination incomplete, trying next method")
                    continue
                else:
                    logger.info(f"   {method} returned no jobs, trying next method")
                    continue
                    
            except Exception as e:
                logger.warning(f"   {method} error: {e}")
                continue

        if not all_jobs:
            logger.info("No jobs found via API")
            return []

        logger.info(f"   Collected {len(all_jobs)} total jobs from API")

        # === 3. Filter by date ===
        filtered_jobs = []
        for job in all_jobs:
            pub_str = job.get("published")
            if pub_str:
                if pub_str.endswith("Z"):
                    pub_str = pub_str[:-1] + "+00:00"
                try:
                    pub_date = datetime.fromisoformat(pub_str)
                    if pub_date >= cutoff_date:
                        filtered_jobs.append(job)
                except:
                    filtered_jobs.append(job)  # Include if can't parse
            else:
                filtered_jobs.append(job)  # Include if no date

        logger.info(f"   After date filter: {len(filtered_jobs)} jobs (last {days} days)")

        if not filtered_jobs:
            logger.info(f"   No jobs within last {days} days")
            return []

        # === 4. Deduplicate by title + merge locations ===
        job_map = {}
        
        for job in filtered_jobs:
            title = job.get("title", "").strip()
            if not title:
                continue

            if title not in job_map:
                locations = job.get("locations", [])
                countries = {loc.get("country") for loc in locations if loc.get("country")}
                
                job_map[title] = {
                    "title": title,
                    "shortcode": job["shortcode"],
                    "countries": countries,
                    "published": job.get("published", ""),
                    "remote": job.get("remote", False),
                    "department": job.get("department", []),
                    "type": job.get("type"),
                    "workplace": job.get("workplace")
                }
            else:
                # Merge countries for duplicate titles
                locations = job.get("locations", [])
                new_countries = {loc.get("country") for loc in locations if loc.get("country")}
                job_map[title]["countries"].update(new_countries)

        # === 4. Build final list ===
        final_jobs = []
        for title, data in job_map.items():
            url = urljoin(board_url, f"j/{data['shortcode']}/")
            countries_list = sorted(data["countries"])
            
            final_jobs.append({
                "url": url,
                "title": title,
                "countries": countries_list,
                "country_count": len(countries_list),
                "remote": data["remote"],
                "published": data["published"][:10] if data["published"] else "",
                "method": "api"
            })
            
            logger.info(f"   JOB: {title[:50]}... → {len(countries_list)} countries")

        logger.info(f"✅ FINAL: {len(final_jobs)} unique jobs (last {days} days)")
        return final_jobs

    except Exception as e:
        logger.error(f"API error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def get_links_from_dom_v5(board_url: str):
    """ULTRA-ROBUST DOM scraper with multiple strategies"""
    
    # Clean URL - remove hash fragments
    clean_url = board_url.split('#')[0].rstrip('/')
    
    for attempt in range(MAX_RETRIES):
        all_links = []
        try:
            logger.info(f"🌐 DOM attempt {attempt + 1}")
            
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-dev-shm-usage',
                        '--no-sandbox'
                    ]
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                
                page = context.new_page()
                
                try:
                    page.goto(clean_url, wait_until="domcontentloaded", timeout=40000)
                except PlaywrightTimeout:
                    logger.warning("   Navigation timeout, continuing...")
                
                page.wait_for_timeout(3000)
                
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except:
                    pass
                
                page.wait_for_timeout(2000)
                
                # Dismiss overlays
                overlay_selectors = [
                    "[data-ui='cookie-consent'] button",
                    "button:has-text('Accept')",
                    "button:has-text('Accept All')",
                    "[data-ui='backdrop']"
                ]
                
                for sel in overlay_selectors:
                    try:
                        btn = page.query_selector(sel)
                        if btn and btn.is_visible():
                            btn.click(timeout=2000)
                            page.wait_for_timeout(1000)
                    except:
                        pass
                
                # STRATEGY 1: Find job elements
                job_selectors = [
                    "li[data-ui='job']",
                    "li[data-ui='job-opening']",
                    "li[data-ui='job-board-position']",
                    ".job-item",
                    "[role='listitem']",
                    "div[data-ui='job-card']"
                ]
                
                job_elements = []
                selected_selector = None
                for selector in job_selectors:
                    try:
                        elements = page.query_selector_all(selector)
                        if elements:
                            job_elements = elements
                            selected_selector = selector
                            logger.info(f"   Found {len(elements)} with: {selector}")
                            break
                    except:
                        continue
                
                # STRATEGY 2: Search all links with /j/
                if not job_elements:
                    logger.info("   No job elements, searching all links...")
                    try:
                        all_page_links = page.query_selector_all("a[href]")
                        for link in all_page_links:
                            href = link.get_attribute("href")
                            if href and '/j/' in href and len(href.split('/j/')[1].split('/')[0]) > 5:
                                full_url = urljoin(clean_url, href)
                                if full_url not in all_links:
                                    all_links.append(full_url)
                        
                        if all_links:
                            logger.info(f"   ✓ Found {len(all_links)} links via href search")
                    except Exception as e:
                        logger.debug(f"   Link search error: {e}")
                
                # STRATEGY 3: Check for "no jobs" message
                if not job_elements and not all_links:
                    page_text = page.inner_text('body').lower()
                    no_jobs_indicators = [
                        "no open positions",
                        "no positions available",
                        "currently no openings",
                        "no jobs at the moment",
                        "not hiring"
                    ]
                    
                    if any(indicator in page_text for indicator in no_jobs_indicators):
                        logger.info("   ℹ️ Page shows no open positions")
                        browser.close()
                        return []
                
                # Process job elements - COLLECT ALL INITIALLY
                if job_elements:
                    cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
                    
                    logger.info(f"   Processing {len(job_elements)} initial job elements...")
                    for item in job_elements:
                        try:
                            link_el = None
                            for link_sel in ["a[aria-labelledby]", "a[href*='/j/']", "a"]:
                                link_el = item.query_selector(link_sel)
                                if link_el:
                                    break
                            
                            if not link_el:
                                continue

                            href = link_el.get_attribute("href")
                            if not href or '/j/' not in href:
                                continue
                            
                            # Check age
                            posted_el = item.query_selector("[data-ui='job-posted']")
                            if posted_el:
                                text = posted_el.inner_text().strip()
                                match = re.search(r"(\d+)\s+day", text)
                                if match and int(match.group(1)) > MAX_DAYS:
                                    continue

                            full_url = urljoin(clean_url, href)
                            if full_url not in all_links:
                                all_links.append(full_url)
                                
                        except:
                            continue
                    
                    logger.info(f"   Collected {len(all_links)} jobs initially")
                
                # Try "Load More" buttons
                load_more_selectors = [
                    "button[data-ui='load-more-button']",
                    "button:has-text('Show more')",
                    "button:has-text('Load more')",
                    "button:has-text('View more jobs')"
                ]
                
                previous_count = len(all_links)
                consecutive_no_change = 0
                max_no_change = 3
                
                for load_attempt in range(20):
                    # Check if button exists and is visible
                    btn_found = False
                    for btn_selector in load_more_selectors:
                        try:
                            btn = page.query_selector(btn_selector)
                            if btn and btn.is_visible():
                                btn_found = True
                                logger.info(f"   Clicking '{btn_selector}' ({load_attempt + 1})...")
                                
                                # Scroll button into view
                                btn.scroll_into_view_if_needed()
                                page.wait_for_timeout(500)
                                
                                # Click and wait
                                btn.click(force=True, timeout=5000)
                                page.wait_for_timeout(5000)  # Longer wait
                                
                                # Get current count before re-scanning
                                before_scan = len(all_links)
                                
                                # Re-scan ALL job elements
                                new_elements = page.query_selector_all(selected_selector)
                                logger.info(f"   Page now has {len(new_elements)} total job elements")
                                
                                # Process ALL elements (not just new ones)
                                all_links = []  # Reset to avoid duplicates
                                for item in new_elements:
                                    try:
                                        link_el = None
                                        for link_sel in ["a[aria-labelledby]", "a[href*='/j/']", "a"]:
                                            link_el = item.query_selector(link_sel)
                                            if link_el:
                                                break
                                        
                                        if link_el:
                                            href = link_el.get_attribute("href")
                                            if href and '/j/' in href:
                                                full_url = urljoin(clean_url, href)
                                                if full_url not in all_links:
                                                    all_links.append(full_url)
                                    except:
                                        continue
                                
                                job_elements = new_elements
                                
                                # Check if we got new jobs
                                if len(all_links) > before_scan:
                                    new_count = len(all_links) - before_scan
                                    logger.info(f"   ✓ Found {new_count} new jobs (total: {len(all_links)})")
                                    consecutive_no_change = 0
                                else:
                                    consecutive_no_change += 1
                                    logger.info(f"   No new jobs ({consecutive_no_change}/{max_no_change})")
                                    if consecutive_no_change >= max_no_change:
                                        logger.info(f"   Stopping - no new jobs after {max_no_change} attempts")
                                        break
                                
                                break  # Break from selector loop
                        except Exception as e:
                            logger.debug(f"   Button click error: {e}")
                            continue
                    
                    if not btn_found or consecutive_no_change >= max_no_change:
                        if not btn_found:
                            logger.info("   No load more button found")
                        break
                
                browser.close()
                
                if all_links:
                    logger.info(f"✅ DOM: {len(all_links)} unique links")
                    return list(set(all_links))
                else:
                    if attempt < MAX_RETRIES - 1:
                        delay = RETRY_DELAY_BASE * (2 ** attempt)
                        logger.info(f"   Retry in {delay}s...")
                        time.sleep(delay)
                        continue
                    return []
                    
        except Exception as e:
            logger.error(f"DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue
    
    return []

# === GET JOB DETAILS ===

def extract_job_with_dom(job_url: str, account: str, shortcode: str):
    """Extract job details from DOM"""
    
    for attempt in range(MAX_RETRIES):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    viewport={'width': 1920, 'height': 1080}
                )
                
                page = context.new_page()
                page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(3000)
                
                # Check for error
                body_text = page.inner_text('body').lower()
                if "error" in body_text and len(body_text) < 100:
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(3000)
                    body_text = page.inner_text('body').lower()
                
                if "error" in body_text and len(body_text) < 100:
                    browser.close()
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return None
                
                # Dismiss overlays
                for sel in ["[data-ui='backdrop']", "[data-ui='cookie-consent'] button"]:
                    try:
                        elem = page.query_selector(sel)
                        if elem and elem.is_visible():
                            elem.click(timeout=1000)
                            page.wait_for_timeout(500)
                    except:
                        pass

                job_data = {"jobId": shortcode, "url": job_url, "account": account}

                # Title
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
                    title = page.title()
                
                if not title or "error" in title.lower():
                    browser.close()
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return None
                
                job_data["title"] = title

                # Location
                for sel in ['[data-ui="job-location"]', 'span[itemprop="addressLocality"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["location"] = el.inner_text().strip()
                            break
                    except:
                        pass

                # Department
                for sel in ['[data-ui="job-department"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["department"] = el.inner_text().strip()
                            break
                    except:
                        pass

                # Type
                for sel in ['[data-ui="job-type"]', 'span[itemprop="employmentType"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["type"] = el.inner_text().strip()
                            break
                    except:
                        pass

                # Workplace
                try:
                    el = page.query_selector('[data-ui="job-workplace"]')
                    if el:
                        job_data["workplace"] = el.inner_text().strip()
                except:
                    pass

                # Description - Get FULL HTML
                description_found = False
                for sel in ['[data-ui="job-description"]', 'div[itemprop="description"]', 'section[data-ui="job-description"]', 'div.description']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 100:
                                job_data["description"] = html
                                logger.info(f"✓ Description: {len(html)} chars (FULL HTML)")
                                description_found = True
                                break
                    except:
                        continue
                
                # Fallback: Get entire job content if specific description not found
                if not description_found:
                    try:
                        # Try to get the entire job content area
                        content_selectors = [
                            'main',
                            'article',
                            '[role="main"]',
                            'div[class*="content"]'
                        ]
                        for sel in content_selectors:
                            el = page.query_selector(sel)
                            if el:
                                html = el.inner_html()
                                if html and len(html) > 500:
                                    job_data["description"] = html
                                    logger.info(f"✓ Description (full content): {len(html)} chars")
                                    description_found = True
                                    break
                    except:
                        pass

                # Requirements - Get FULL HTML
                for sel in ['[data-ui="job-requirements"]', 'section:has-text("Requirements")', 'div:has-text("Requirements")', 'section:has-text("Qualifications")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["requirements"] = html
                                logger.info(f"✓ Requirements: {len(html)} chars (FULL HTML)")
                                break
                    except:
                        continue

                # Benefits - Get FULL HTML
                for sel in ['[data-ui="job-benefits"]', 'section:has-text("Benefits")', 'div:has-text("Benefits")', 'section:has-text("What we offer")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["benefits"] = html
                                logger.info(f"✓ Benefits: {len(html)} chars (FULL HTML)")
                                break
                    except:
                        continue

                # JSON-LD
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
                except:
                    pass

                browser.close()
                
                has_content = bool(job_data.get("description") or job_data.get("location"))
                
                if has_content:
                    return job_data
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return job_data if title else None
                    
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue
    
    return None

def extract_job_with_api(account: str, shortcode: str, job_url: str):
    """Extract job from API - supports POST and GET"""
    
    endpoints = {
        "v3": f"https://apply.workable.com/api/v3/accounts/{account}/jobs/{shortcode}",
        "v2": f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{shortcode}",
    }
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    
    for name, endpoint in endpoints.items():
        # Try POST then GET
        for method in ["POST", "GET"]:
            try:
                if method == "POST":
                    resp = requests.post(endpoint, json={}, headers=headers, timeout=15)
                else:
                    resp = requests.get(endpoint, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    time.sleep(5)
                    continue
                
                if resp.status_code == 200:
                    d = resp.json()
                    logger.info(f"✅ API {name} ({method}) SUCCESS")
                    
                    return {
                        "jobId": d.get("shortcode") or shortcode,
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
                    }
                
                elif resp.status_code in [404, 410]:
                    break
                
            except:
                continue
    
    return None

# === ENDPOINTS ===

@app.get("/get-job-links")
def get_job_links(
    url: str = Query(..., description="Workable board URL"),
    days: int = Query(5, description="Get jobs from last N days (use 0 for all jobs)")
):
    """
    Get job links with deduplication and country merging
    - Tries POST/GET API first
    - Falls back to DOM scraping
    - Returns unique jobs by title with merged countries
    
    Set days=0 to get ALL jobs (no date filtering)
    """
    logger.info(f"\n{'='*80}")
    if days == 0:
        logger.info(f"🎯 GET LINKS: {url} (ALL JOBS)")
    else:
        logger.info(f"🎯 GET LINKS: {url} (last {days} days)")
    logger.info(f"{'='*80}")
    
    try:
        if is_lever_url(url):
            links = get_links_from_lever_api(url, days=days)
            if links:
                return {"success": True, "total": len(links), "jobs": links, "method": "api"}

        # Try API
        links = get_links_from_api(url, days=days if days > 0 else 9999)  # Use 9999 days for "all"
        
        if links:
            return {"success": True, "total": len(links), "jobs": links, "method": "api"}
        
        # Try DOM
        logger.info("API failed, trying DOM...")
        dom_links = get_links_from_dom_v5(url)
        
        if dom_links:
            # Convert to same format as API
            jobs = [{"url": link, "method": "dom"} for link in dom_links]
            return {"success": True, "total": len(jobs), "jobs": jobs, "method": "dom"}
        
        # No jobs
        return {"success": True, "total": 0, "jobs": [], "note": "No jobs found"}
        
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"success": False, "total": 0, "jobs": [], "error": str(e)}

@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable or Lever job URL")):
    """Get full job details - DOM first, then API fallback"""
    logger.info(f"\n🎯 GET DETAILS: {url}")
    
    try:
        if is_lever_url(url):
            api_result = extract_job_with_lever_api(url)
            if api_result and api_result.get("title"):
                api_result["method"] = "api"
                return {"success": True, "job": api_result}
            return {"success": False, "error": "Lever API failed", "url": url}

        account, shortcode = _parse_workable_url(url)
        
        # Try DOM
        dom_result = extract_job_with_dom(url, account, shortcode)
        if dom_result and dom_result.get("title"):
            dom_result["method"] = "dom"
            return {"success": True, "job": dom_result}
        
        # Try API
        time.sleep(2)
        api_result = extract_job_with_api(account, shortcode, url)
        if api_result and api_result.get("title"):
            api_result["method"] = "api"
            return {"success": True, "job": api_result}
        
        # Final DOM
        time.sleep(3)
        final_result = extract_job_with_dom(url, account, shortcode)
        if final_result and final_result.get("title"):
            final_result["method"] = "dom_final"
            return {"success": True, "job": final_result}
        
        return {"success": False, "error": "All methods failed", "url": url}
        
    except Exception as e:
        return {"success": False, "error": str(e), "url": url}

@app.get("/health")
def health():
    return {"status": "healthy", "version": "5.0-production"}

@app.get("/")
def root():
    return {
        "name": "Workable + Lever Scraper v5 - Production",
        "version": "5.0",
        "improvements": [
            "✨ POST API support (80%+ success rate boost)",
            "🧲 Lever API support",
            "🔄 Job deduplication by title",
            "🌍 Country/location merging",
            "🍪 Cloudflare cookie support",
            "📅 Configurable date range (default 5 days)",
            "🎯 3-strategy DOM fallback"
        ],
        "endpoints": {
            "/get-job-links": "GET ?url=BOARD_URL&days=5 → Unique jobs with merged countries",
            "/get-job-details": "GET ?url=JOB_URL → Full job details"
        }
    }
