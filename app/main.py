# app/main.py - Your working code + Rate limiting for n8n
from fastapi import FastAPI, Query, HTTPException
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
import requests
from datetime import datetime, timedelta, timezone
import re
import logging
import json
import time
import random
from collections import defaultdict

from app.lever_scraper import extract_job_with_lever_api, get_links_from_lever_api, is_lever_url

app = FastAPI()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2

# ============================================================================
# RATE LIMITING FOR N8N (NEW - added for your 10K jobs)
# ============================================================================

class GlobalRateLimiter:
    """
    Global rate limiter that persists across n8n requests
    This prevents n8n from overwhelming Workable/Lever with requests
    """
    
    def __init__(self):
        self.request_times = defaultdict(list)
        self.domain_cooldowns = {}
        self.consecutive_errors = defaultdict(int)
        
        # Settings - adjust these if needed
        self.max_requests_per_5min = 8
        self.min_delay_seconds = 5
        self.max_delay_seconds = 10
        self.cooldown_minutes = 15
        
    def wait_if_needed(self, domain: str):
        """
        Wait if needed before making request
        Raises HTTPException(429) if cooldown is too long
        """
        now = datetime.now()
        
        # Check cooldown
        if domain in self.domain_cooldowns:
            cooldown_end = self.domain_cooldowns[domain]
            if now < cooldown_end:
                wait_seconds = (cooldown_end - now).total_seconds()
                
                if wait_seconds > 300:  # If more than 5 minutes, tell n8n to retry later
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "error": "rate_limit_cooldown",
                            "retry_after_seconds": int(wait_seconds),
                            "message": f"In cooldown. Retry after {int(wait_seconds)} seconds"
                        }
                    )
                
                logger.warning(f"⏸️  Cooldown: waiting {wait_seconds:.0f}s")
                time.sleep(wait_seconds)
                del self.domain_cooldowns[domain]
        
        # Clean old requests
        cutoff = now - timedelta(minutes=5)
        self.request_times[domain] = [t for t in self.request_times[domain] if t > cutoff]
        
        # Check rate limit
        recent_requests = len(self.request_times[domain])
        
        if recent_requests >= self.max_requests_per_5min:
            oldest = self.request_times[domain][0]
            wait_time = 300 - (now - oldest).total_seconds()
            
            if wait_time > 0:
                logger.info(f"⏳ Rate limit: {recent_requests}/{self.max_requests_per_5min}. Waiting {wait_time:.0f}s")
                time.sleep(wait_time + random.uniform(2, 5))
                
                # Clean again
                now = datetime.now()
                cutoff = now - timedelta(minutes=5)
                self.request_times[domain] = [t for t in self.request_times[domain] if t > cutoff]
        
        # Add random delay
        delay = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        logger.info(f"⏱️  Random delay: {delay:.1f}s")
        time.sleep(delay)
        
        # Record request
        self.request_times[domain].append(now)
    
    def record_rate_limit(self, domain: str):
        """Record that we got rate limited"""
        self.consecutive_errors[domain] += 1
        cooldown_multiplier = min(self.consecutive_errors[domain], 4)
        cooldown_minutes = self.cooldown_minutes * cooldown_multiplier
        
        self.domain_cooldowns[domain] = datetime.now() + timedelta(minutes=cooldown_minutes)
        logger.error(f"🚫 Rate limited! Cooldown: {cooldown_minutes} min")
    
    def record_success(self, domain: str):
        """Record successful request"""
        if domain in self.consecutive_errors and self.consecutive_errors[domain] > 0:
            self.consecutive_errors[domain] -= 1
    
    def get_stats(self, domain: str) -> dict:
        """Get stats for monitoring"""
        now = datetime.now()
        cutoff = now - timedelta(minutes=5)
        recent = [t for t in self.request_times[domain] if t > cutoff]
        
        in_cooldown = False
        cooldown_remaining = 0
        
        if domain in self.domain_cooldowns:
            cooldown_end = self.domain_cooldowns[domain]
            if now < cooldown_end:
                in_cooldown = True
                cooldown_remaining = (cooldown_end - now).total_seconds()
        
        return {
            "domain": domain,
            "requests_last_5min": len(recent),
            "max_requests_per_5min": self.max_requests_per_5min,
            "in_cooldown": in_cooldown,
            "cooldown_remaining_seconds": int(cooldown_remaining),
            "consecutive_errors": self.consecutive_errors[domain]
        }

# Global instance
rate_limiter = GlobalRateLimiter()

# ============================================================================
# YOUR ORIGINAL HELPER FUNCTIONS (unchanged)
# ============================================================================

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

# ============================================================================
# YOUR ORIGINAL SCRAPING FUNCTIONS (unchanged)
# ============================================================================

def get_links_from_api(board_url: str, days: int = 5):
    """Get jobs from LAST {days} DAYS using POST/GET methods"""
    try:
        clean_url = board_url.split('#')[0].rstrip('/')
        subdomain = clean_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"🔌 API: Fetching jobs from last {days} days...")
        logger.info(f"   Subdomain: {subdomain}")

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
                        logger.warning(f"   {method} pagination failed with 400")
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

                        all_jobs.append(job)
                    
                    if not page_has_recent_jobs and page_num > 1:
                        logger.info(f"   Page {page_num} has no recent jobs, stopping")
                        break

                    next_page = data.get("nextPage")
                    if not next_page:
                        break

                    time.sleep(0.5)

                if all_jobs and not pagination_incomplete:
                    logger.info(f"✅ {method} fetched {len(all_jobs)} jobs successfully")
                    break
                elif all_jobs and pagination_incomplete:
                    logger.info(f"⚠ {method} fetched {len(all_jobs)} jobs but incomplete")
                    continue
                else:
                    logger.info(f"   {method} returned no jobs")
                    continue
                    
            except Exception as e:
                logger.warning(f"   {method} error: {e}")
                continue

        if not all_jobs:
            return []

        logger.info(f"   Collected {len(all_jobs)} total jobs")

        # Filter by date
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
                    filtered_jobs.append(job)
            else:
                filtered_jobs.append(job)

        logger.info(f"   After date filter: {len(filtered_jobs)} jobs")

        if not filtered_jobs:
            return []

        # Deduplicate
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
                locations = job.get("locations", [])
                new_countries = {loc.get("country") for loc in locations if loc.get("country")}
                job_map[title]["countries"].update(new_countries)

        # Build final list
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

        logger.info(f"✅ FINAL: {len(final_jobs)} unique jobs")
        return final_jobs

    except Exception as e:
        logger.error(f"API error: {e}")
        return []

def get_links_from_dom_v5(board_url: str):
    """ULTRA-ROBUST DOM scraper"""
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
                    logger.warning("   Navigation timeout")
                
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
                
                # Find job elements
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
                
                # Search all links
                if not job_elements:
                    logger.info("   Searching all links...")
                    try:
                        all_page_links = page.query_selector_all("a[href]")
                        for link in all_page_links:
                            href = link.get_attribute("href")
                            if href and '/j/' in href and len(href.split('/j/')[1].split('/')[0]) > 5:
                                full_url = urljoin(clean_url, href)
                                if full_url not in all_links:
                                    all_links.append(full_url)
                        
                        if all_links:
                            logger.info(f"   ✓ Found {len(all_links)} links")
                    except Exception as e:
                        logger.debug(f"   Link search error: {e}")
                
                # Check for "no jobs"
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
                        logger.info("   ℹ️  No open positions")
                        browser.close()
                        return []
                
                # Process job elements
                if job_elements:
                    cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
                    
                    logger.info(f"   Processing {len(job_elements)} elements...")
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
                    
                    logger.info(f"   Collected {len(all_links)} jobs")
                
                # Try "Load More"
                load_more_selectors = [
                    "button[data-ui='load-more-button']",
                    "button:has-text('Show more')",
                    "button:has-text('Load more')",
                    "button:has-text('View more jobs')"
                ]
                
                consecutive_no_change = 0
                max_no_change = 3
                
                for load_attempt in range(20):
                    btn_found = False
                    for btn_selector in load_more_selectors:
                        try:
                            btn = page.query_selector(btn_selector)
                            if btn and btn.is_visible():
                                btn_found = True
                                logger.info(f"   Clicking '{btn_selector}' ({load_attempt + 1})...")
                                
                                btn.scroll_into_view_if_needed()
                                page.wait_for_timeout(500)
                                btn.click(force=True, timeout=5000)
                                page.wait_for_timeout(5000)
                                
                                before_scan = len(all_links)
                                new_elements = page.query_selector_all(selected_selector)
                                logger.info(f"   Page now has {len(new_elements)} elements")
                                
                                all_links = []
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
                                
                                if len(all_links) > before_scan:
                                    new_count = len(all_links) - before_scan
                                    logger.info(f"   ✓ Found {new_count} new jobs (total: {len(all_links)})")
                                    consecutive_no_change = 0
                                else:
                                    consecutive_no_change += 1
                                    logger.info(f"   No new jobs ({consecutive_no_change}/{max_no_change})")
                                    if consecutive_no_change >= max_no_change:
                                        break
                                
                                break
                        except Exception as e:
                            logger.debug(f"   Button error: {e}")
                            continue
                    
                    if not btn_found or consecutive_no_change >= max_no_change:
                        break
                
                browser.close()
                
                if all_links:
                    logger.info(f"✅ DOM: {len(all_links)} links")
                    return list(set(all_links))
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return []
                    
        except Exception as e:
            logger.error(f"DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue
    
    return []

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

                # Description
                description_found = False
                for sel in ['[data-ui="job-description"]', 'div[itemprop="description"]', 'section[data-ui="job-description"]', 'div.description']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 100:
                                job_data["description"] = html
                                description_found = True
                                break
                    except:
                        continue
                
                if not description_found:
                    try:
                        content_selectors = ['main', 'article', '[role="main"]', 'div[class*="content"]']
                        for sel in content_selectors:
                            el = page.query_selector(sel)
                            if el:
                                html = el.inner_html()
                                if html and len(html) > 500:
                                    job_data["description"] = html
                                    break
                    except:
                        pass

                # Requirements
                for sel in ['[data-ui="job-requirements"]', 'section:has-text("Requirements")', 'div:has-text("Requirements")', 'section:has-text("Qualifications")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["requirements"] = html
                                break
                    except:
                        continue

                # Benefits
                for sel in ['[data-ui="job-benefits"]', 'section:has-text("Benefits")', 'div:has-text("Benefits")', 'section:has-text("What we offer")']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 50:
                                job_data["benefits"] = html
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
    """Extract job from API"""
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

## ENDPOINTS (with rate limiting added)
@app.get("/get-job-links")
def get_job_links(
    url: str = Query(..., description="Workable or Lever board URL"),
    days: int = Query(5, description="Get jobs from last N days")
):
    """Get job links with rate limiting"""
    logger.info(f"\n{'='*80}")
    if days == 0:
        logger.info(f"🎯 GET LINKS: {url} (ALL JOBS)")
    else:
        logger.info(f"🎯 GET LINKS: {url} (last {days} days)")
    logger.info(f"{'='*80}")
    try:
        domain = urlparse(url).netloc
        
        # RATE LIMIT (NEW)
        rate_limiter.wait_if_needed(domain)
        
        if is_lever_url(url):
            links = get_links_from_lever_api(url, days=days)
            if links:
                rate_limiter.record_success(domain)
                return {"success": True, "total": len(links), "jobs": links, "method": "api"}

        links = get_links_from_api(url, days=days if days > 0 else 9999)
        
        if links:
            rate_limiter.record_success(domain)
            return {"success": True, "total": len(links), "jobs": links, "method": "api"}
        
        logger.info("API failed, trying DOM...")
        dom_links = get_links_from_dom_v5(url)
        
        if dom_links:
            jobs = [{"url": link, "method": "dom"} for link in dom_links]
            rate_limiter.record_success(domain)
            return {"success": True, "total": len(jobs), "jobs": jobs, "method": "dom"}
        
        return {"success": True, "total": 0, "jobs": [], "note": "No jobs found"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"success": False, "total": 0, "jobs": [], "error": str(e)}
@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable or Lever job URL")):
    """Get full job details with rate limiting"""
    logger.info(f"\n🎯 GET DETAILS: {url}")
    try:
        domain = urlparse(url).netloc
        
        # RATE LIMIT (NEW)
        rate_limiter.wait_if_needed(domain)
        
        if is_lever_url(url):
            api_result = extract_job_with_lever_api(url)
            if api_result and api_result.get("title"):
                api_result["method"] = "api"
                rate_limiter.record_success(domain)
                return {"success": True, "job": api_result}
            return {"success": False, "error": "Lever API failed", "url": url}

        account, shortcode = _parse_workable_url(url)
        
        dom_result = extract_job_with_dom(url, account, shortcode)
        if dom_result and dom_result.get("title"):
            dom_result["method"] = "dom"
            rate_limiter.record_success(domain)
            return {"success": True, "job": dom_result}
        
        time.sleep(2)
        api_result = extract_job_with_api(account, shortcode, url)
        if api_result and api_result.get("title"):
            api_result["method"] = "api"
            rate_limiter.record_success(domain)
            return {"success": True, "job": api_result}
        
        time.sleep(3)
        final_result = extract_job_with_dom(url, account, shortcode)
        if final_result and final_result.get("title"):
            final_result["method"] = "dom_final"
            rate_limiter.record_success(domain)
            return {"success": True, "job": final_result}
        
        return {"success": False, "error": "All methods failed", "url": url}
        
    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e), "url": url}
@app.get("/rate-limit-status")
def rate_limit_status(domain: str = Query("apply.workable.com")):
    """Check rate limit status - useful for monitoring"""
    stats = rate_limiter.get_stats(domain)
    return {
        "success": True,
        "stats": stats,
        "can_proceed": not stats["in_cooldown"]
    }
@app.post("/reset-rate-limit")
def reset_rate_limit(domain: str = Query(...)):
    """Reset rate limits for a domain"""
    if domain in rate_limiter.domain_cooldowns:
        del rate_limiter.domain_cooldowns[domain]
    rate_limiter.consecutive_errors[domain] = 0
    rate_limiter.request_times[domain] = []

    return {"success": True, "message": f"Rate limit reset for {domain}"}
@app.get("/health")
def health():
    return {
    "status": "healthy",
    "version": "6.0-n8n-rate-limited",
    "rate_limiter": {
    "max_requests_per_5min": rate_limiter.max_requests_per_5min,
    "min_delay_seconds": rate_limiter.min_delay_seconds,
    "max_delay_seconds": rate_limiter.max_delay_seconds
    }
    }
@app.get("/")
def root():
    return {
    "name": "Workable + Lever Scraper v6 - N8N Optimized",
    "version": "6.0",
    "improvements": [
    "🛡️  Global rate limiter for n8n (NEW)",
    "⏱️  5-10 second delays between requests (NEW)",
    "🚫 Auto 15-min cooldowns on rate limits (NEW)",
    "✨ POST API support",
    "🧲 Lever API support",
    "🔄 Job deduplication",
    "🌍 Country merging",
    "🍪 Cloudflare cookies",
    ],
    "n8n_usage": {
    "main_endpoint": "/get-job-details?url=JOB_URL",
    "check_status": "/rate-limit-status?domain=apply.workable.com",
    "note": "Rate limiting happens automatically - just call the API from n8n"
    },
    "endpoints": {
    "/get-job-links": "GET ?url=BOARD_URL&days=5",
    "/get-job-details": "GET ?url=JOB_URL",
    "/rate-limit-status": "GET ?domain=DOMAIN",
    "/reset-rate-limit": "POST ?domain=DOMAIN",
    "/health": "GET"
    }
}