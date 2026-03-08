# app/main.py
from dotenv import load_dotenv
load_dotenv()

# app/main.py
from fastapi import FastAPI, Query, HTTPException
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth.core import StealthConfig  # pip install tf-playwright-stealth
from playwright_stealth import stealth_sync
import requests
from datetime import datetime, timedelta, timezone
import re
import logging
import json
import time
import random
import os
from collections import defaultdict

from app.lever_scraper import extract_job_with_lever_api, get_links_from_lever_api, is_lever_url

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2

# ============================================================================
# BRIGHT DATA CONFIG - set these in Railway environment variables
# ============================================================================

BD_HOST     = os.getenv("BD_HOST", "brd.superproxy.io")
BD_PORT     = int(os.getenv("BD_PORT", "33335"))
BD_USERNAME = os.getenv("BD_USERNAME", "")   # brd-customer-XXXX-zone-YOURZONE
BD_PASSWORD = os.getenv("BD_PASSWORD", "")

# True if credentials are set, False = fall back to direct connection
USE_BRIGHT_DATA = bool(BD_USERNAME and BD_PASSWORD)

REQUESTS_CA_BUNDLE = (os.getenv("REQUESTS_CA_BUNDLE") or "").strip()
ALLOW_INSECURE_PROXY_SSL = (os.getenv("ALLOW_INSECURE_PROXY_SSL") or "0").strip() == "1"

# IMPORTANT: insecure proxy mode must override everything
if USE_BRIGHT_DATA and ALLOW_INSECURE_PROXY_SSL:
    REQUESTS_VERIFY = False
elif REQUESTS_CA_BUNDLE:
    REQUESTS_VERIFY = REQUESTS_CA_BUNDLE
else:
    REQUESTS_VERIFY = True

logger.warning(
    f"[BOOT] USE_BRIGHT_DATA={USE_BRIGHT_DATA} "
    f"REQUESTS_VERIFY={REQUESTS_VERIFY!r} "
    f"CA_BUNDLE={REQUESTS_CA_BUNDLE!r} "
    f"ALLOW_INSECURE_PROXY_SSL={ALLOW_INSECURE_PROXY_SSL}"
)

logger.warning(f"[BOOT] USE_BRIGHT_DATA={USE_BRIGHT_DATA} REQUESTS_VERIFY={REQUESTS_VERIFY!r} CA_BUNDLE={REQUESTS_CA_BUNDLE!r} ALLOW_INSECURE_PROXY_SSL={ALLOW_INSECURE_PROXY_SSL}")

def _bd_proxy_url() -> str:
    """Return a rotating Bright Data proxy URL (new session per call)"""
    session_id = random.randint(1000000, 9999999)
    return f"http://{BD_USERNAME}-session-{session_id}:{BD_PASSWORD}@{BD_HOST}:{BD_PORT}"

def _bd_requests_proxies() -> dict:
    """Return proxy dict for requests library"""
    proxy_url = _bd_proxy_url()
    return {"http": proxy_url, "https": proxy_url}

# ============================================================================
# RATE LIMITER
# ============================================================================

class GlobalRateLimiter:
    """Persists across n8n requests to prevent rate limiting"""

    def __init__(self):
        self.request_times = defaultdict(list)
        self.domain_cooldowns = {}
        self.consecutive_errors = defaultdict(int)
        self.max_requests_per_5min = 8
        self.min_delay_seconds = 5
        self.max_delay_seconds = 10
        self.cooldown_minutes = 15

    def wait_if_needed(self, domain: str):
        """Wait before making a request. Raises 429 if cooldown > 5 min."""
        now = datetime.now()

        # Check cooldown
        if domain in self.domain_cooldowns:
            cooldown_end = self.domain_cooldowns[domain]
            if now < cooldown_end:
                wait_seconds = (cooldown_end - now).total_seconds()
                if wait_seconds > 300:
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "error": "rate_limit_cooldown",
                            "retry_after_seconds": int(wait_seconds),
                            "message": f"In cooldown. Retry after {int(wait_seconds)}s"
                        }
                    )
                logger.warning(f"⏸️  Cooldown: waiting {wait_seconds:.0f}s")
                time.sleep(wait_seconds)
                del self.domain_cooldowns[domain]

        # Remove requests older than 5 min
        cutoff = now - timedelta(minutes=5)
        self.request_times[domain] = [t for t in self.request_times[domain] if t > cutoff]

        # Check rate limit
        recent = len(self.request_times[domain])
        if recent >= self.max_requests_per_5min:
            oldest = self.request_times[domain][0]
            wait_time = 300 - (now - oldest).total_seconds()
            if wait_time > 0:
                logger.info(f"⏳ Rate limit: {recent}/{self.max_requests_per_5min}. Waiting {wait_time:.0f}s")
                time.sleep(wait_time + random.uniform(2, 5))
                now = datetime.now()
                cutoff = now - timedelta(minutes=5)
                self.request_times[domain] = [t for t in self.request_times[domain] if t > cutoff]

        # Random human-like delay
        delay = random.uniform(self.min_delay_seconds, self.max_delay_seconds)
        logger.info(f"⏱️  Delay: {delay:.1f}s")
        time.sleep(delay)
        self.request_times[domain].append(datetime.now())

    def record_rate_limit(self, domain: str):
        self.consecutive_errors[domain] += 1
        mins = self.cooldown_minutes * min(self.consecutive_errors[domain], 4)
        self.domain_cooldowns[domain] = datetime.now() + timedelta(minutes=mins)
        logger.error(f"🚫 Rate limited! Cooldown: {mins} min")

    def record_success(self, domain: str):
        if self.consecutive_errors[domain] > 0:
            self.consecutive_errors[domain] -= 1

    def get_stats(self, domain: str) -> dict:
        now = datetime.now()
        cutoff = now - timedelta(minutes=5)
        recent = [t for t in self.request_times[domain] if t > cutoff]
        in_cooldown, cooldown_remaining = False, 0
        if domain in self.domain_cooldowns:
            end = self.domain_cooldowns[domain]
            if now < end:
                in_cooldown = True
                cooldown_remaining = (end - now).total_seconds()
        return {
            "domain": domain,
            "requests_last_5min": len(recent),
            "max_requests_per_5min": self.max_requests_per_5min,
            "in_cooldown": in_cooldown,
            "cooldown_remaining_seconds": int(cooldown_remaining),
            "consecutive_errors": self.consecutive_errors[domain]
        }

rate_limiter = GlobalRateLimiter()

# ============================================================================
# STEALTH BROWSER HELPER
# ============================================================================

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]

VIEWPORTS = [
    {'width': 1366, 'height': 768},
    {'width': 1440, 'height': 900},
    {'width': 1920, 'height': 1080},
    {'width': 1536, 'height': 864},
]

def new_stealth_page(p):
    """
    Launch a stealth browser routed through Bright Data proxy.
    Falls back to direct connection if credentials not set.
    Always call browser.close() after use.
    """
    launch_args = [
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-gpu',
    ]

    # Configure stealth
    stealth_config = StealthConfig(
        webdriver=True,
        chrome_app=True,
        chrome_csi=True,
        chrome_load_times=True,
        chrome_runtime=True,
        iframe_content_window=True,
        media_codecs=True,
        navigator_hardware_concurrency=True,
        navigator_languages=True,
        navigator_permissions=True,
        navigator_plugins=True,
        navigator_user_agent=True,
        navigator_vendor=True,
        webgl_vendor=True,
        outerdimensions=True,
    )

    if USE_BRIGHT_DATA:
        # Route browser traffic through Bright Data residential proxy
        logger.info(f"🌐 Using Bright Data proxy")
        browser = p.chromium.launch(
            headless=True,
            args=launch_args,
            proxy={"server": f"http://{BD_HOST}:{BD_PORT}"}
        )
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport=random.choice(VIEWPORTS),
            locale=random.choice(['en-US', 'en-GB', 'en-CA']),
            timezone_id=random.choice(['America/New_York', 'America/Chicago', 'Europe/London']),
            ignore_https_errors=True,
            http_credentials={"username": f"{BD_USERNAME}-session-{random.randint(1000000,9999999)}", "password": BD_PASSWORD}
        )
    else:
        logger.warning("⚠️  No Bright Data credentials - using direct connection")
        browser = p.chromium.launch(headless=True, args=launch_args)
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport=random.choice(VIEWPORTS),
            locale=random.choice(['en-US', 'en-GB', 'en-CA']),
            timezone_id=random.choice(['America/New_York', 'America/Chicago', 'Europe/London']),
        )

    page = context.new_page()

    # Apply stealth using tf-playwright-stealth API
    stealth_sync(page, stealth_config)
    
    return browser, page

# ============================================================================
# HELPERS
# ============================================================================

def _parse_workable_url(job_url: str):
    """
    Supports:
      - https://apply.workable.com/<account>/j/<shortcode>/
      - https://jobs.workable.com/company/<companyKey>/j/<shortcode>/
    Returns: (account_or_companykey, shortcode, kind)
      kind: "apply" | "jobs_by_workable"
    """
    u = urlparse(job_url)
    parts = [p for p in (u.path or "").strip("/").split("/") if p]

    # Find /j/<shortcode>
    try:
        j_idx = parts.index("j")
        shortcode = parts[j_idx + 1]
    except Exception:
        raise ValueError(f"Cannot parse Workable URL: {job_url}")

    # Jobs-by-Workable format: /company/<companyKey>/j/<shortcode>
    if len(parts) >= 4 and parts[0] == "company" and parts[2] == "j":
        company_key = parts[1]
        return company_key, shortcode, "jobs_by_workable"

    # Classic format: /<account>/j/<shortcode>
    # (or sometimes /<account>/jobs/<...>/j/<shortcode>, but most common is this)
    account = parts[j_idx - 1] if j_idx - 1 >= 0 else None
    if not account or account == "company":
        raise ValueError(f"Cannot determine account for: {job_url}")

    return account, shortcode, "apply"
def _dismiss_overlays(page):
    for sel in [
        "[data-ui='cookie-consent'] button",
        "button:has-text('Accept')",
        "button:has-text('Accept All')",
        "[data-ui='backdrop']"
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click(timeout=2000)
                page.wait_for_timeout(random.randint(500, 1000))
        except:
            pass

def _extract_jobboard_initial_state(html: str):
    """
    Extract window.jobBoard.initialState as JSON.

    Note: window.jobBoard itself is a JS object (not valid JSON),
    but initialState is JSON-compatible on Jobs-by-Workable pages.
    """
    marker = "window.jobBoard"
    pos = html.find(marker)
    if pos == -1:
        return None

    # Find initialState key
    pos = html.find("initialState", pos)
    if pos == -1:
        return None

    # Find the first '{' after initialState
    start = html.find("{", pos)
    if start == -1:
        return None

    # Brace matching
    depth = 0
    end = None
    for i in range(start, len(html)):
        c = html[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end is None:
        return None

    blob = html[start:end]
    try:
        return json.loads(blob)
    except Exception as e:
        logger.warning(f"initialState json parse failed: {e}")
        return None

def get_links_from_embedded_jobboard(board_url: str) -> list:
    """Get job links from embedded window.jobBoard.initialState JSON (Jobs by Workable)"""
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            # Avoid Brotli if possible (simplifies decoding)
            "Accept-Encoding": "gzip, deflate",
        }

        proxies = _bd_requests_proxies() if USE_BRIGHT_DATA else {}

        resp = requests.get(
            board_url,
            headers=headers,
            timeout=30,
            proxies=proxies,
            verify=REQUESTS_VERIFY,
        )

        logger.info(
            f"Embedded fetch status={resp.status_code} "
            f"encoding={resp.headers.get('Content-Encoding')} "
            f"verify={REQUESTS_VERIFY!r}"
        )

        if resp.status_code != 200:
            logger.warning(f"Embedded jobboard status != 200: {resp.status_code}")
            return []

        # Decode body safely (handles rare cases if br still appears)
        raw = resp.content
        enc = (resp.headers.get("Content-Encoding") or "").lower()

        if enc == "br":
            try:
                import brotli  # pip install brotli (optional)
                raw = brotli.decompress(raw)
            except Exception as e:
                logger.warning(f"Brotli decompress failed (install brotli): {e}")
                return []

        html = raw.decode(resp.encoding or "utf-8", errors="replace")

        state = _extract_jobboard_initial_state(html)
        if not state:
            logger.warning("Embedded jobboard: initialState not found")
            return []

        # Find company key inside initialState
        company_key = None
        for k in state.keys():
            if k.startswith("api/v1/companies/"):
                company_key = k
                break

        if not company_key:
            logger.warning("Embedded jobboard: api/v1/companies key not found")
            return []

        data = state.get(company_key, {}).get("data", {})
        jobs = data.get("jobs", [])
        if not jobs:
            logger.warning("Embedded jobboard: jobs list empty")
            return []

        links = []
        for job in jobs:
            if not isinstance(job, dict):
                continue

            job_url = job.get("url") or job.get("application_url") or job.get("apply_url")
            if not job_url:
                shortcode = job.get("shortcode") or job.get("code")
                if shortcode:
                    # best-effort link building (works for many boards)
                    job_url = urljoin(board_url if board_url.endswith("/") else board_url + "/", f"j/{shortcode}/")

            if job_url and job_url not in links:
                links.append(job_url)

        if links:
            logger.info(f"✅ Embedded jobboard: {len(links)} links")
        return links

    except Exception as e:
        logger.warning(f"Embedded jobboard exception: {e}")
        return []

# ============================================================================
# GET JOB LINKS - STEALTH DOM FIRST, API FALLBACK
# ============================================================================

def get_links_from_dom_stealth(board_url: str) -> list:
    """Get job links using stealth browser (PRIMARY method)"""
    clean_url = board_url.split('#')[0].rstrip('/')

    for attempt in range(MAX_RETRIES):
        all_links = []
        try:
            logger.info(f"🥷 Stealth DOM attempt {attempt + 1}")

            with sync_playwright() as p:
                browser, page = new_stealth_page(p)

                try:
                    page.goto(clean_url, wait_until="domcontentloaded", timeout=40000)
                except PlaywrightTimeout:
                    logger.warning("   Navigation timeout, continuing...")

                page.wait_for_timeout(random.randint(3000, 5000))
                try:
                    page.wait_for_load_state("networkidle", timeout=10000)
                except:
                    pass
                page.wait_for_timeout(random.randint(1000, 2000))

                _dismiss_overlays(page)

                # Strategy 1: Job element selectors
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

                # Strategy 2: Scan all /j/ links
                if not job_elements:
                    logger.info("   Scanning all /j/ links...")
                    try:
                        for link in page.query_selector_all("a[href]"):
                            href = link.get_attribute("href")
                            if href and '/j/' in href and len(href.split('/j/')[1].split('/')[0]) > 5:
                                full_url = urljoin(clean_url, href)
                                if full_url not in all_links:
                                    all_links.append(full_url)
                        if all_links:
                            logger.info(f"   ✓ Found {len(all_links)} links")
                    except Exception as e:
                        logger.debug(f"   Link scan error: {e}")

                # Strategy 3: Check for no jobs
                if not job_elements and not all_links:
                    page_text = page.inner_text('body').lower()
                    if any(x in page_text for x in ["no open positions", "no positions available", "currently no openings", "not hiring"]):
                        logger.info("   ℹ️  No open positions")
                        browser.close()
                        return []

                # Process job elements
                if job_elements:
                    for item in job_elements:
                        try:
                            link_el = None
                            for s in ["a[aria-labelledby]", "a[href*='/j/']", "a"]:
                                link_el = item.query_selector(s)
                                if link_el:
                                    break
                            if not link_el:
                                continue
                            href = link_el.get_attribute("href")
                            if not href or '/j/' not in href:
                                continue
                            posted_el = item.query_selector("[data-ui='job-posted']")
                            if posted_el:
                                match = re.search(r"(\d+)\s+day", posted_el.inner_text().strip())
                                if match and int(match.group(1)) > MAX_DAYS:
                                    continue
                            full_url = urljoin(clean_url, href)
                            if full_url not in all_links:
                                all_links.append(full_url)
                        except:
                            continue

                    logger.info(f"   Collected {len(all_links)} jobs")

                    # Load More
                    load_more_selectors = [
                        "button[data-ui='load-more-button']",
                        "button:has-text('Show more')",
                        "button:has-text('Load more')",
                        "button:has-text('View more jobs')"
                    ]

                    consecutive_no_change = 0
                    for _ in range(20):
                        btn_found = False
                        for btn_sel in load_more_selectors:
                            try:
                                btn = page.query_selector(btn_sel)
                                if btn and btn.is_visible():
                                    btn_found = True
                                    btn.scroll_into_view_if_needed()
                                    page.wait_for_timeout(random.randint(500, 1000))
                                    btn.click(force=True, timeout=5000)
                                    page.wait_for_timeout(random.randint(5000, 8000))

                                    before = len(all_links)
                                    new_elements = page.query_selector_all(selected_selector)
                                    all_links = []
                                    for item in new_elements:
                                        try:
                                            link_el = None
                                            for s in ["a[aria-labelledby]", "a[href*='/j/']", "a"]:
                                                link_el = item.query_selector(s)
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

                                    if len(all_links) > before:
                                        logger.info(f"   ✓ {len(all_links) - before} new jobs (total: {len(all_links)})")
                                        consecutive_no_change = 0
                                    else:
                                        consecutive_no_change += 1
                                        if consecutive_no_change >= 3:
                                            break
                                    break
                            except Exception as e:
                                logger.debug(f"   Load more error: {e}")
                                continue

                        if not btn_found or consecutive_no_change >= 3:
                            break

                browser.close()

                if all_links:
                    logger.info(f"✅ Stealth DOM: {len(all_links)} links")
                    return list(set(all_links))
                elif attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_BASE * (2 ** attempt))

        except Exception as e:
            logger.error(f"Stealth DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))

    return []


def get_links_from_api(board_url: str, days: int = 5) -> list:
    """Get job links via Workable API (FALLBACK method)"""
    try:
        clean_url = board_url.split('#')[0].rstrip('/')
        subdomain = clean_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"

        logger.info(f"🔌 API fallback: fetching jobs...")

        # Get Cloudflare cookies via stealth browser
        cookies = {}
        try:
            with sync_playwright() as p:
                browser, page = new_stealth_page(p)
                page.goto(clean_url, wait_until="domcontentloaded", timeout=60000)
                try:
                    page.wait_for_load_state("load", timeout=60000)
                except:
                    pass
                page.wait_for_timeout(random.randint(3000, 5000))
                try:
                    page.click("text=Accept", timeout=3000)
                except:
                    pass
                for c in page.context.cookies():
                    if c["name"] in ["cf_clearance", "__cf_bm", "wmc"]:
                        cookies[c["name"]] = c["value"]
                browser.close()
        except Exception as e:
            logger.warning(f"Cookie extraction failed: {e}")

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": random.choice(USER_AGENTS),
            "Origin": "https://apply.workable.com",
            "Referer": board_url,
        }
        if cookies:
            headers["Cookie"] = "; ".join([f"{k}={v}" for k, v in cookies.items()])

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        all_jobs = []

        for method in ["POST", "GET"]:
            try:
                logger.info(f"   Trying {method}...")
                next_page = None
                page_num = 0
                all_jobs = []
                incomplete = False

                while page_num < 100:
                    page_num += 1

                    proxies = _bd_requests_proxies() if USE_BRIGHT_DATA else {}

                    if method == "POST":
                        payload = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
                        if next_page:
                            payload["nextPage"] = next_page
                        resp = requests.post(
                            api_url,
                            json=payload,
                            headers=headers,
                            timeout=20,
                            proxies=proxies,
                            verify=REQUESTS_VERIFY,
                        )
                    else:
                        params = {"limit": 50}
                        if next_page:
                            params["nextPage"] = next_page
                        resp = requests.get(
                            api_url,
                            params=params,
                            headers=headers,
                            timeout=20,
                            proxies=proxies,
                            verify=REQUESTS_VERIFY,
                        )

                    if resp.status_code == 429:
                        logger.warning("   API rate limited")
                        time.sleep(10)
                        continue
                    if resp.status_code == 400:
                        incomplete = True
                        break
                    if resp.status_code != 200:
                        logger.warning(f"   {method} failed: {resp.status_code}")
                        break

                    data = resp.json()
                    results = data.get("results", [])
                    if not results:
                        break

                    logger.info(f"   Page {page_num}: {len(results)} jobs")
                    page_has_recent = False

                    for job in results:
                        pub_str = job.get("published")
                        if pub_str:
                            if pub_str.endswith("Z"):
                                pub_str = pub_str[:-1] + "+00:00"
                            try:
                                pub_date = datetime.fromisoformat(pub_str)
                                if pub_date >= cutoff_date:
                                    page_has_recent = True
                            except:
                                pass
                        all_jobs.append(job)

                    if not page_has_recent and page_num > 1:
                        break

                    next_page = data.get("nextPage")
                    if not next_page:
                        break

                    time.sleep(random.uniform(1, 2))

                if all_jobs and not incomplete:
                    logger.info(f"✅ API {method}: {len(all_jobs)} jobs")
                    break

            except Exception as e:
                logger.warning(f"   {method} error: {e}")
                continue

        if not all_jobs:
            return []

        # Filter by date
        filtered = []
        for job in all_jobs:
            pub_str = job.get("published")
            if pub_str:
                if pub_str.endswith("Z"):
                    pub_str = pub_str[:-1] + "+00:00"
                try:
                    pub_date = datetime.fromisoformat(pub_str)
                    if pub_date >= cutoff_date:
                        filtered.append(job)
                except:
                    filtered.append(job)
            else:
                filtered.append(job)

        if not filtered:
            return []

        # Deduplicate by title + merge countries
        job_map = {}
        for job in filtered:
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
                    "type": job.get("type"),
                    "workplace": job.get("workplace")
                }
            else:
                locs = job.get("locations", [])
                job_map[title]["countries"].update(
                    loc.get("country") for loc in locs if loc.get("country")
                )

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

        logger.info(f"✅ API FINAL: {len(final_jobs)} unique jobs")
        return final_jobs

    except Exception as e:
        logger.error(f"API error: {e}")
        return []


# ============================================================================
# GET JOB DETAILS - STEALTH DOM FIRST, API FALLBACK
# ============================================================================

def extract_job_with_dom_stealth(job_url: str, account: str, shortcode: str):
    """Extract job details using stealth browser (PRIMARY method)"""
    for attempt in range(MAX_RETRIES):
        try:
            with sync_playwright() as p:
                browser, page = new_stealth_page(p)

                page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(random.randint(3000, 5000))

                # Check error page
                body_text = page.inner_text('body').lower()
                if "error" in body_text and len(body_text) < 100:
                    page.reload(wait_until="domcontentloaded")
                    page.wait_for_timeout(random.randint(3000, 5000))
                    body_text = page.inner_text('body').lower()
                    if "error" in body_text and len(body_text) < 100:
                        browser.close()
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                            continue
                        return None

                _dismiss_overlays(page)

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
                try:
                    el = page.query_selector('[data-ui="job-department"]')
                    if el:
                        job_data["department"] = el.inner_text().strip()
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
                desc_found = False
                for sel in ['[data-ui="job-description"]', 'div[itemprop="description"]', 'section[data-ui="job-description"]', 'div.description']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            html = el.inner_html()
                            if html and len(html) > 100:
                                job_data["description"] = html
                                desc_found = True
                                break
                    except:
                        continue

                if not desc_found:
                    for sel in ['main', 'article', '[role="main"]', 'div[class*="content"]']:
                        try:
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
                elif attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                    continue
                return job_data if title else None

        except Exception as e:
            logger.error(f"Stealth DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue

    return None


def extract_job_with_api(account: str, shortcode: str, job_url: str):
    """Extract job details from Workable API (FALLBACK method)"""
    endpoints = {
        "v3": f"https://apply.workable.com/api/v3/accounts/{account}/jobs/{shortcode}",
        "v2": f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{shortcode}",
    }

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": random.choice(USER_AGENTS)
    }

    proxies = _bd_requests_proxies() if USE_BRIGHT_DATA else {}

    for name, endpoint in endpoints.items():
        for method in ["POST", "GET"]:
            try:
                resp = requests.post(
                    endpoint,
                    json={},
                    headers=headers,
                    timeout=15,
                    proxies=proxies,
                    verify=REQUESTS_VERIFY,
                ) \
                    if method == "POST" else \
                    requests.get(
                        endpoint,
                        headers=headers,
                        timeout=15,
                        proxies=proxies,
                        verify=REQUESTS_VERIFY,
                    )

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


# ============================================================================
# ENDPOINTS
# ============================================================================

@app.get("/get-job-links")
def get_job_links(
    url: str = Query(..., description="Workable or Lever board URL"),
    days: int = Query(5, description="Get jobs from last N days (0 = all)")
):
    """Get job links - Stealth DOM first, API fallback"""
    logger.info(f"\n{'='*60}")
    logger.info(f"🎯 GET LINKS: {url}")
    logger.info(f"{'='*60}")

    try:
        domain = urlparse(url).netloc
        rate_limiter.wait_if_needed(domain)

        # Lever
        if is_lever_url(url):
            links = get_links_from_lever_api(url, days=days)
            if links:
                rate_limiter.record_success(domain)
                return {"success": True, "total": len(links), "jobs": links, "method": "lever_api"}

        # 1st: Stealth DOM
        links = get_links_from_dom_stealth(url)
        if links:
            rate_limiter.record_success(domain)
            jobs = [{"url": l, "method": "stealth_dom"} for l in links]
            return {"success": True, "total": len(jobs), "jobs": jobs, "method": "stealth_dom"}

        # 2nd: Embedded JobBoard JSON (Jobs by Workable)
        logger.info("Stealth DOM failed, trying embedded jobboard...")
        links = get_links_from_embedded_jobboard(url)
        if links:
            rate_limiter.record_success(domain)
            jobs = [{"url": l, "method": "embedded_jobboard"} for l in links]
            return {"success": True, "total": len(jobs), "jobs": jobs, "method": "embedded_jobboard"}

        # 3rd: Workable API fallback
        logger.info("Embedded jobboard failed, trying API...")
        links = get_links_from_api(url, days=days if days > 0 else 9999)
        if links:
            rate_limiter.record_success(domain)
            return {"success": True, "total": len(links), "jobs": links, "method": "api"}

        return {"success": True, "total": 0, "jobs": [], "note": "No jobs found"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"success": False, "total": 0, "jobs": [], "error": str(e)}


@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable or Lever job URL")):
    """Get full job details - Stealth DOM first, API fallback"""
    logger.info(f"\n🎯 GET DETAILS: {url}")

    try:
        domain = urlparse(url).netloc
        rate_limiter.wait_if_needed(domain)

        # Lever
        if is_lever_url(url):
            result = extract_job_with_lever_api(url)
            if result and result.get("title"):
                result["method"] = "lever_api"
                rate_limiter.record_success(domain)
                return {"success": True, "job": result}
            return {"success": False, "error": "Lever API failed", "url": url}

        account, shortcode = _parse_workable_url(url)

        # 1st: Stealth DOM
        result = extract_job_with_dom_stealth(url, account, shortcode)
        if result and result.get("title"):
            result["method"] = "stealth_dom"
            rate_limiter.record_success(domain)
            return {"success": True, "job": result}

        # 2nd: API fallback
        logger.info("Stealth DOM failed, trying API...")
        time.sleep(random.uniform(2, 4))
        result = extract_job_with_api(account, shortcode, url)
        if result and result.get("title"):
            result["method"] = "api"
            rate_limiter.record_success(domain)
            return {"success": True, "job": result}

        return {"success": False, "error": "All methods failed", "url": url}

    except HTTPException:
        raise
    except Exception as e:
        return {"success": False, "error": str(e), "url": url}


@app.get("/rate-limit-status")
def rate_limit_status(domain: str = Query("apply.workable.com")):
    """Check rate limit status"""
    stats = rate_limiter.get_stats(domain)
    return {"success": True, "stats": stats, "can_proceed": not stats["in_cooldown"]}


@app.post("/reset-rate-limit")
def reset_rate_limit(domain: str = Query(...)):
    """Reset rate limits for a domain"""
    if domain in rate_limiter.domain_cooldowns:
        del rate_limiter.domain_cooldowns[domain]
    rate_limiter.consecutive_errors[domain] = 0
    rate_limiter.request_times[domain] = []
    return {"success": True, "message": f"Reset for {domain}"}


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "version": "7.0-stealth-brightdata",
        "bright_data": {
            "enabled": USE_BRIGHT_DATA,
            "host": BD_HOST if USE_BRIGHT_DATA else None,
            "port": BD_PORT if USE_BRIGHT_DATA else None,
        },
        "scraping_order": ["stealth_dom", "api_fallback"],
        "rate_limiter": {
            "max_requests_per_5min": rate_limiter.max_requests_per_5min,
            "min_delay_seconds": rate_limiter.min_delay_seconds,
            "max_delay_seconds": rate_limiter.max_delay_seconds
        }
    }


@app.get("/")
def root():
    return {
        "name": "Workable + Lever Scraper v7 - Stealth + Bright Data",
        "version": "7.0",
        "bright_data_enabled": USE_BRIGHT_DATA,
        "scraping_order": {
            "get_job_links": ["1. Lever API (if Lever URL)", "2. Stealth DOM via Bright Data", "3. Workable API via Bright Data"],
            "get_job_details": ["1. Lever API (if Lever URL)", "2. Stealth DOM via Bright Data", "3. Workable API via Bright Data"]
        },
        "improvements": [
            "🌐 Bright Data residential proxy (rotating IPs)",
            "🥷 playwright-stealth (hides headless signals)",
            "🔄 DOM is primary, API is fallback",
            "🛡️  Global rate limiter for n8n",
            "⏱️  5-10s random delays",
            "🚫 Auto cooldowns on rate limits",
        ],
        "env_vars_required": [
            "BD_HOST (default: brd.superproxy.io)",
            "BD_PORT (default: 33335)",
            "BD_USERNAME (brd-customer-XXXX-zone-YOURZONE)",
            "BD_PASSWORD (your zone password)"
        ],
        "endpoints": {
            "/get-job-links": "GET ?url=BOARD_URL&days=5",
            "/get-job-details": "GET ?url=JOB_URL",
            "/rate-limit-status": "GET ?domain=DOMAIN",
            "/reset-rate-limit": "POST ?domain=DOMAIN",
            "/health": "GET"
        }
    }
