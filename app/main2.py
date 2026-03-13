# app/main.py
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, Query, HTTPException
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth.core import StealthConfig
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
from app.checkpoint import (
    get_checkpoint,
    save_checkpoint,
    clear_checkpoint,
    list_checkpoints,
    compute_since_dt,
)

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2

# ============================================================================
# BRIGHT DATA CONFIG
# ============================================================================

BD_HOST     = os.getenv("BD_HOST", "brd.superproxy.io")
BD_PORT     = int(os.getenv("BD_PORT", "33335"))
BD_USERNAME = os.getenv("BD_USERNAME", "")
BD_PASSWORD = os.getenv("BD_PASSWORD", "")

USE_BRIGHT_DATA = bool(BD_USERNAME and BD_PASSWORD)

REQUESTS_CA_BUNDLE = (os.getenv("REQUESTS_CA_BUNDLE") or "").strip()
ALLOW_INSECURE_PROXY_SSL = (os.getenv("ALLOW_INSECURE_PROXY_SSL") or "0").strip() == "1"

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


def _bd_proxy_url() -> str:
    session_id = random.randint(1000000, 9999999)
    return f"http://{BD_USERNAME}-session-{session_id}:{BD_PASSWORD}@{BD_HOST}:{BD_PORT}"


def _bd_requests_proxies() -> dict:
    proxy_url = _bd_proxy_url()
    return {"http": proxy_url, "https": proxy_url}


# ============================================================================
# RATE LIMITER
# ============================================================================

class GlobalRateLimiter:
    def __init__(self):
        self.request_times = defaultdict(list)
        self.domain_cooldowns = {}
        self.consecutive_errors = defaultdict(int)
        self.max_requests_per_5min = 8
        self.min_delay_seconds = 5
        self.max_delay_seconds = 10
        self.cooldown_minutes = 15

    def wait_if_needed(self, domain: str):
        now = datetime.now()
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

        cutoff = now - timedelta(minutes=5)
        self.request_times[domain] = [t for t in self.request_times[domain] if t > cutoff]

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


def new_stealth_page(p, use_proxy: bool | None = None):
    if use_proxy is None:
        use_proxy = USE_BRIGHT_DATA
    launch_args = [
        '--disable-blink-features=AutomationControlled',
        '--disable-dev-shm-usage',
        '--no-sandbox',
        '--disable-gpu',
    ]

    stealth_config = StealthConfig(
        webdriver=True, chrome_app=True, chrome_csi=True, chrome_load_times=True,
        chrome_runtime=True, iframe_content_window=True, media_codecs=True,
        navigator_hardware_concurrency=True, navigator_languages=True,
        navigator_permissions=True, navigator_plugins=True, navigator_user_agent=True,
        navigator_vendor=True, webgl_vendor=True, outerdimensions=True,
    )

    if use_proxy:
        logger.info(f"🌐 Using Bright Data proxy")
        browser = p.chromium.launch(
            headless=True, args=launch_args,
            proxy={"server": f"http://{BD_HOST}:{BD_PORT}"}
        )
        context = browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport=random.choice(VIEWPORTS),
            locale=random.choice(['en-US', 'en-GB', 'en-CA']),
            timezone_id=random.choice(['America/New_York', 'America/Chicago', 'Europe/London']),
            ignore_https_errors=True,
            http_credentials={
                "username": f"{BD_USERNAME}-session-{random.randint(1000000,9999999)}",
                "password": BD_PASSWORD
            }
        )
    else:
        if USE_BRIGHT_DATA:
            logger.warning("⚠️  Bright Data disabled for this attempt - using direct connection")
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
    stealth_sync(page, stealth_config)
    return browser, page


# ============================================================================
# HELPERS
# ============================================================================

def _parse_workable_url(job_url: str):
    u = urlparse(job_url)
    parts = [p for p in (u.path or "").strip("/").split("/") if p]
    try:
        j_idx = parts.index("j")
        shortcode = parts[j_idx + 1]
    except Exception:
        raise ValueError(f"Cannot parse Workable URL: {job_url}")

    if len(parts) >= 4 and parts[0] == "company" and parts[2] == "j":
        company_key = parts[1]
        return company_key, shortcode, "jobs_by_workable"

    account = parts[j_idx - 1] if j_idx - 1 >= 0 else None
    if not account or account == "company":
        raise ValueError(f"Cannot determine account for: {job_url}")

    return account, shortcode, "apply"


def _is_job_within_range(pub_str: str, since_dt: datetime) -> bool:
    """Return True if the job's published date is >= since_dt."""
    if not pub_str or not since_dt:
        return True  # no filter → include everything
    try:
        if pub_str.endswith("Z"):
            pub_str = pub_str[:-1] + "+00:00"
        pub_date = datetime.fromisoformat(pub_str)
        if pub_date.tzinfo is None:
            pub_date = pub_date.replace(tzinfo=timezone.utc)
        return pub_date >= since_dt
    except Exception:
        return True  # unparseable → include


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


def _scroll_and_collect_links(page, base_url: str, max_scrolls: int = 6) -> list:
    links = []
    for _ in range(max_scrolls):
        try:
            for link in page.query_selector_all("a[href]"):
                href = link.get_attribute("href")
                if href and "/j/" in href and len(href.split("/j/")[1].split("/")[0]) > 5:
                    full_url = urljoin(base_url, href)
                    if full_url not in links:
                        links.append(full_url)
            if links:
                return links
        except Exception:
            pass
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass
        page.wait_for_timeout(random.randint(1500, 2500))
    return links


def _click_load_more(page, selectors: list[str]) -> bool:
    try:
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    except Exception:
        pass
    page.wait_for_timeout(random.randint(800, 1200))

    for sel in selectors:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.scroll_into_view_if_needed()
                page.wait_for_timeout(random.randint(500, 900))
                try:
                    btn.click(force=True, timeout=5000)
                except Exception:
                    try:
                        page.evaluate("(s) => { const b = document.querySelector(s); if (b) b.click(); }", sel)
                    except Exception:
                        pass
                return True
        except Exception:
            continue
    return False


def _links_from_workable_results(results: list, base_url: str) -> list:
    links = []
    for job in results or []:
        if not isinstance(job, dict):
            continue
        job_url = job.get("url") or job.get("application_url") or job.get("apply_url")
        if not job_url:
            shortcode = job.get("shortcode") or job.get("code")
            if shortcode:
                job_url = urljoin(
                    base_url if base_url.endswith("/") else base_url + "/",
                    f"j/{shortcode}/"
                )
        if job_url and job_url not in links:
            links.append(job_url)
    return links


def _account_from_board_url(board_url: str) -> str | None:
    try:
        u = urlparse(board_url)
        host = (u.netloc or "").lower()
        parts = [p for p in (u.path or "").strip("/").split("/") if p]
        if host == "apply.workable.com" and parts:
            return parts[0]
    except Exception:
        return None
    return None


def _extract_jobboard_initial_state(html: str):
    marker = "window.jobBoard"
    pos = html.find(marker)
    if pos == -1:
        return None
    pos = html.find("initialState", pos)
    if pos == -1:
        return None
    start = html.find("{", pos)
    if start == -1:
        return None
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


def get_links_from_embedded_jobboard(board_url: str, since_dt: datetime = None) -> list:
    try:
        host = urlparse(board_url).netloc.lower()
        if host != "jobs.workable.com":
            return []

        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
        }
        proxies = _bd_requests_proxies() if USE_BRIGHT_DATA else {}
        resp = requests.get(board_url, headers=headers, timeout=30, proxies=proxies, verify=REQUESTS_VERIFY)

        if resp.status_code != 200:
            return []

        raw = resp.content
        enc = (resp.headers.get("Content-Encoding") or "").lower()
        if enc == "br":
            try:
                import brotli
                raw = brotli.decompress(raw)
            except Exception:
                return []

        html = raw.decode(resp.encoding or "utf-8", errors="replace")
        state = _extract_jobboard_initial_state(html)
        if not state:
            return []

        company_key = None
        for k in state.keys():
            if k.startswith("api/v1/companies/"):
                company_key = k
                break
        if not company_key:
            return []

        jobs = state.get(company_key, {}).get("data", {}).get("jobs", [])
        links = []
        for job in jobs:
            if not isinstance(job, dict):
                continue
            # Date filter
            if since_dt and not _is_job_within_range(job.get("published", ""), since_dt):
                continue
            job_url = job.get("url") or job.get("application_url") or job.get("apply_url")
            if not job_url:
                shortcode = job.get("shortcode") or job.get("code")
                if shortcode:
                    job_url = urljoin(
                        board_url if board_url.endswith("/") else board_url + "/",
                        f"j/{shortcode}/"
                    )
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

def get_links_from_dom_stealth(board_url: str, since_dt: datetime = None) -> list:
    clean_url = board_url.split('#')[0].rstrip('/')

    for attempt in range(MAX_RETRIES):
        all_links = []
        proxy_blocked = False
        try:
            logger.info(f"🥷 Stealth DOM attempt {attempt + 1}")

            proxy_modes = [True, False] if USE_BRIGHT_DATA else [False]
            for use_proxy in proxy_modes:
                if not use_proxy and not proxy_blocked:
                    break

                with sync_playwright() as p:
                    browser, page = new_stealth_page(p, use_proxy=use_proxy)
                    api_payloads = []

                    def _capture_api(resp):
                        nonlocal proxy_blocked
                        try:
                            url = resp.url
                            if "apply.workable.com/api/" not in url:
                                return
                            if resp.status == 402:
                                proxy_blocked = True
                                return
                            if resp.status != 200:
                                return
                            data = resp.json()
                            if isinstance(data, dict) and data.get("results"):
                                api_payloads.append(data)
                        except Exception:
                            return

                    page.on("response", _capture_api)

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

                    try:
                        title = (page.title() or "").lower()
                        body = (page.inner_text("body") or "").lower()
                        if any(x in title for x in ["just a moment", "access denied", "attention required"]) or \
                           any(x in body for x in ["just a moment", "access denied", "captcha"]):
                            logger.warning("   Possible block page detected")
                    except:
                        pass

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

                    if not job_elements and not all_links:
                        logger.info("   Scrolling to load jobs...")
                        all_links = _scroll_and_collect_links(page, clean_url)
                        if all_links:
                            logger.info(f"   ✓ Found {len(all_links)} links after scroll")

                    if not job_elements and not all_links and api_payloads:
                        logger.info("   Using in-browser API responses...")
                        for payload in api_payloads:
                            results = payload.get("results", [])
                            links = _links_from_workable_results(results, clean_url)
                            for l in links:
                                if l not in all_links:
                                    all_links.append(l)
                        if all_links:
                            logger.info(f"   ✓ Found {len(all_links)} links from API responses")

                    if not job_elements and not all_links:
                        account = _account_from_board_url(clean_url)
                        if account:
                            try:
                                logger.info("   Using in-browser fetch to API...")
                                api_url = f"https://apply.workable.com/api/v3/accounts/{account}/jobs"
                                payload = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
                                next_page = None
                                for page_idx in range(1, 6):
                                    if next_page:
                                        payload["nextPage"] = next_page
                                    data = page.evaluate(
                                        """
(async ({ apiUrl, payload }) => {
  try {
    const res = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'X-Requested-With': 'XMLHttpRequest'
      },
      credentials: 'include',
      body: JSON.stringify(payload)
    });
    const contentType = res.headers.get('content-type') || '';
    const text = await res.text();
    let json = null;
    if (contentType.includes('application/json')) {
      try { json = JSON.parse(text); } catch (e) {}
    }
    return { ok: res.ok, status: res.status, contentType, json, bodySnippet: text.slice(0, 200) };
  } catch (e) {
    return { ok: false, status: 0, error: String(e) };
  }
})
""",
                                        {"apiUrl": api_url, "payload": payload},
                                    )
                                    if isinstance(data, dict) and data.get("status") == 402:
                                        proxy_blocked = True
                                        break
                                    if isinstance(data, dict) and data.get("json") and data["json"].get("results"):
                                        links = _links_from_workable_results(data["json"].get("results"), clean_url)
                                        if links:
                                            all_links.extend([l for l in links if l not in all_links])
                                        next_page = data["json"].get("nextPage") or data["json"].get("nextPageToken")
                                        if not next_page:
                                            break
                                    else:
                                        break
                            except Exception as e:
                                logger.debug(f"   In-browser fetch failed: {e}")

                    if not job_elements and not all_links:
                        page_text = page.inner_text('body').lower()
                        if any(x in page_text for x in ["no open positions", "no positions available", "currently no openings", "not hiring"]):
                            logger.info("   ℹ️  No open positions")
                            browser.close()
                            return []

                    if job_elements:
                        stop_pagination = False
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

                                # Date filter via DOM "days ago" badge
                                if since_dt:
                                    posted_el = item.query_selector("[data-ui='job-posted']")
                                    if posted_el:
                                        match = re.search(r"(\d+)\s+day", posted_el.inner_text().strip())
                                        if match:
                                            days_ago = int(match.group(1))
                                            job_dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
                                            if job_dt < since_dt:
                                                # Jobs are listed newest-first; once we're past the cutoff
                                                # we can stop loading more pages
                                                stop_pagination = True
                                                continue

                                full_url = urljoin(clean_url, href)
                                if full_url not in all_links:
                                    all_links.append(full_url)
                            except:
                                continue

                        logger.info(f"   Collected {len(all_links)} jobs (stop_pagination={stop_pagination})")

                        if not stop_pagination:
                            load_more_selectors = [
                                "button[data-ui='load-more-button']",
                                "button:has-text('Show more')",
                                "button:has-text('Load more')",
                                "button:has-text('View more jobs')"
                            ]
                            consecutive_no_change = 0
                            for _ in range(20):
                                clicked = _click_load_more(page, load_more_selectors)
                                if not clicked:
                                    break

                                page.wait_for_timeout(random.randint(4000, 7000))

                                before = len(all_links)
                                new_elements = page.query_selector_all(selected_selector)
                                new_batch = []
                                local_stop = False

                                for item in new_elements:
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

                                        if since_dt:
                                            posted_el = item.query_selector("[data-ui='job-posted']")
                                            if posted_el:
                                                match = re.search(r"(\d+)\s+day", posted_el.inner_text().strip())
                                                if match:
                                                    days_ago = int(match.group(1))
                                                    job_dt = datetime.now(timezone.utc) - timedelta(days=days_ago)
                                                    if job_dt < since_dt:
                                                        local_stop = True
                                                        continue

                                        full_url = urljoin(clean_url, href)
                                        if full_url not in new_batch:
                                            new_batch.append(full_url)
                                    except:
                                        continue

                                all_links = list(set(all_links) | set(new_batch))

                                if local_stop:
                                    logger.info("   ⏹️  Reached date cutoff, stopping pagination")
                                    break

                                if len(all_links) > before:
                                    consecutive_no_change = 0
                                else:
                                    consecutive_no_change += 1
                                    if consecutive_no_change >= 3:
                                        break

                    browser.close()

                if all_links:
                    logger.info(f"✅ Stealth DOM: {len(all_links)} links")
                    return list(set(all_links))

                if use_proxy and proxy_blocked:
                    logger.warning("   Proxy returned 402, retrying direct...")
                    continue

            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))

        except Exception as e:
            logger.error(f"Stealth DOM attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))

    return []


def get_links_from_api(board_url: str, since_dt: datetime = None) -> list:
    """Get job links via Workable API with proper date filtering."""
    try:
        clean_url = board_url.split('#')[0].rstrip('/')
        subdomain = clean_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"

        logger.info(f"🔌 API fallback: fetching jobs...")

        # Cloudflare cookies
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

        all_jobs = []
        best_jobs = []

        for method in ["POST", "GET"]:
            try:
                logger.info(f"   Trying {method}...")
                next_page = None
                page_num = 0
                all_jobs = []

                while page_num < 100:
                    page_num += 1
                    proxies = _bd_requests_proxies() if USE_BRIGHT_DATA else {}

                    def _do_request(use_proxies: dict):
                        if method == "POST":
                            payload = {"query": "", "location": [], "department": [], "worktype": [], "remote": []}
                            if next_page:
                                payload["nextPage"] = next_page
                            return requests.post(api_url, json=payload, headers=headers, timeout=20, proxies=use_proxies, verify=REQUESTS_VERIFY)
                        params = {"limit": 50}
                        if next_page:
                            params["nextPage"] = next_page
                        return requests.get(api_url, params=params, headers=headers, timeout=20, proxies=use_proxies, verify=REQUESTS_VERIFY)

                    resp = _do_request(proxies)
                    if USE_BRIGHT_DATA and resp.status_code in {402, 403, 407}:
                        resp = _do_request({})

                    if resp.status_code == 429:
                        time.sleep(10)
                        continue
                    if resp.status_code in (400, 404):
                        break
                    if resp.status_code != 200:
                        break

                    data = resp.json()
                    results = data.get("results", [])
                    if not results:
                        break

                    logger.info(f"   Page {page_num}: {len(results)} jobs")
                    page_hit_cutoff = False

                    for job in results:
                        pub_str = job.get("published", "")
                        if since_dt and not _is_job_within_range(pub_str, since_dt):
                            # API returns newest-first; once we hit an old job, stop
                            page_hit_cutoff = True
                            continue
                        all_jobs.append(job)

                    if page_hit_cutoff and page_num > 1:
                        logger.info(f"   ⏹️  Reached date cutoff at page {page_num}, stopping")
                        break

                    next_page = data.get("nextPage") or data.get("nextPageToken")
                    if not next_page:
                        break

                    time.sleep(random.uniform(1, 2))

                if all_jobs:
                    best_jobs = all_jobs
                    break

            except Exception as e:
                logger.warning(f"   {method} error: {e}")
                continue

        if not best_jobs:
            return []

        # Deduplicate by title
        job_map = {}
        for job in best_jobs:
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
                job_map[title]["countries"].update(loc.get("country") for loc in locs if loc.get("country"))

        final_jobs = []
        for title, d in job_map.items():
            url = urljoin(board_url, f"j/{d['shortcode']}/")
            countries_list = sorted(d["countries"])
            final_jobs.append({
                "url": url,
                "title": title,
                "countries": countries_list,
                "country_count": len(countries_list),
                "remote": d["remote"],
                "published": d["published"][:10] if d["published"] else "",
                "method": "api"
            })

        logger.info(f"✅ API FINAL: {len(final_jobs)} unique jobs")
        return final_jobs

    except Exception as e:
        logger.error(f"API error: {e}")
        return []


# ============================================================================
# GET JOB DETAILS - unchanged from v7, kept verbatim
# ============================================================================

def extract_job_with_dom_stealth(job_url: str, account: str, shortcode: str):
    for attempt in range(MAX_RETRIES):
        try:
            with sync_playwright() as p:
                browser, page = new_stealth_page(p)

                page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(random.randint(3000, 5000))

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

                for sel in ['[data-ui="job-location"]', 'span[itemprop="addressLocality"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["location"] = el.inner_text().strip()
                            break
                    except:
                        pass

                try:
                    el = page.query_selector('[data-ui="job-department"]')
                    if el:
                        job_data["department"] = el.inner_text().strip()
                except:
                    pass

                for sel in ['[data-ui="job-type"]', 'span[itemprop="employmentType"]']:
                    try:
                        el = page.query_selector(sel)
                        if el:
                            job_data["type"] = el.inner_text().strip()
                            break
                    except:
                        pass

                try:
                    el = page.query_selector('[data-ui="job-workplace"]')
                    if el:
                        job_data["workplace"] = el.inner_text().strip()
                except:
                    pass

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

    return None


def extract_job_with_api(account: str, shortcode: str, job_url: str):
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
                def _do_request(use_proxies: dict):
                    if method == "POST":
                        return requests.post(endpoint, json={}, headers=headers, timeout=15, proxies=use_proxies, verify=REQUESTS_VERIFY)
                    return requests.get(endpoint, headers=headers, timeout=15, proxies=use_proxies, verify=REQUESTS_VERIFY)

                resp = _do_request(proxies)
                if USE_BRIGHT_DATA and resp.status_code in {402, 403, 407}:
                    resp = _do_request({})

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
    days: float = Query(0, description="Fetch jobs posted within last N days (e.g. 5, 1.5). 0 = use checkpoint or all"),
    hours: float = Query(0, description="Fetch jobs posted within last N hours (e.g. 24). Overrides days if set"),
    use_checkpoint: bool = Query(True, description="Resume from last saved checkpoint when days/hours=0"),
    save_progress: bool = Query(True, description="Save checkpoint after successful run"),
):
    """
    Get job links with flexible date filtering and checkpoint/resume support.

    Date filter priority:
      1. hours > 0  →  last N hours
      2. days  > 0  →  last N days
      3. use_checkpoint=True  →  resume from last run's newest job
      4. no filter  →  fetch everything
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"🎯 GET LINKS: {url}  days={days}  hours={hours}  use_checkpoint={use_checkpoint}")
    logger.info(f"{'='*60}")

    try:
        domain = urlparse(url).netloc
        rate_limiter.wait_if_needed(domain)

        # Resolve effective since_dt
        since_dt = compute_since_dt(
            days=days,
            hours=hours,
            board_url=url,
            use_checkpoint=use_checkpoint,
        )

        since_str = since_dt.isoformat() if since_dt else None
        logger.info(f"📅 Effective since_dt: {since_str}")

        # ---- Lever ----
        if is_lever_url(url):
            links = get_links_from_lever_api(url, days=int(days) if days > 0 else (int(hours / 24) + 1 if hours > 0 else 9999))
            if links:
                # Lever API returns full job objects; filter by since_dt
                if since_dt:
                    links = [j for j in links if _is_job_within_range(j.get("published", ""), since_dt)]
                rate_limiter.record_success(domain)
                _maybe_save_checkpoint(url, links, save_progress)
                return {"success": True, "total": len(links), "jobs": links, "method": "lever_api", "since": since_str}

        # ---- 1st: Stealth DOM ----
        links = get_links_from_dom_stealth(url, since_dt=since_dt)
        if links:
            rate_limiter.record_success(domain)
            jobs = [{"url": l, "method": "stealth_dom"} for l in links]
            _maybe_save_checkpoint(url, jobs, save_progress)
            return {"success": True, "total": len(jobs), "jobs": jobs, "method": "stealth_dom", "since": since_str}

        # ---- 2nd: Embedded JobBoard JSON ----
        logger.info("Stealth DOM failed, trying embedded jobboard...")
        links = get_links_from_embedded_jobboard(url, since_dt=since_dt)
        if links:
            rate_limiter.record_success(domain)
            jobs = [{"url": l, "method": "embedded_jobboard"} for l in links]
            _maybe_save_checkpoint(url, jobs, save_progress)
            return {"success": True, "total": len(jobs), "jobs": jobs, "method": "embedded_jobboard", "since": since_str}

        # ---- 3rd: Workable API ----
        logger.info("Embedded jobboard failed, trying API...")
        links = get_links_from_api(url, since_dt=since_dt)
        if links:
            rate_limiter.record_success(domain)
            _maybe_save_checkpoint(url, links, save_progress)
            return {"success": True, "total": len(links), "jobs": links, "method": "api", "since": since_str}

        return {"success": True, "total": 0, "jobs": [], "note": "No jobs found", "since": since_str}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"success": False, "total": 0, "jobs": [], "error": str(e)}


def _maybe_save_checkpoint(board_url: str, jobs: list, save_progress: bool):
    """Save checkpoint using the newest published date from this run's jobs."""
    if not save_progress or not jobs:
        return
    try:
        # Find the most recently published job
        newest_pub = ""
        newest_url = ""
        newest_title = ""
        for job in jobs:
            pub = job.get("published", "") if isinstance(job, dict) else ""
            if pub and pub > newest_pub:
                newest_pub = pub
                newest_url = job.get("url", "") if isinstance(job, dict) else ""
                newest_title = job.get("title", "") if isinstance(job, dict) else ""

        save_checkpoint(
            board_url,
            last_job_url=newest_url,
            last_job_title=newest_title,
            last_published=newest_pub,
            total_jobs_seen=len(jobs),
        )
    except Exception as e:
        logger.warning(f"[checkpoint] Failed to save: {e}")


@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable or Lever job URL")):
    logger.info(f"\n🎯 GET DETAILS: {url}")

    try:
        domain = urlparse(url).netloc
        rate_limiter.wait_if_needed(domain)

        if is_lever_url(url):
            result = extract_job_with_lever_api(url)
            if result and result.get("title"):
                result["method"] = "lever_api"
                rate_limiter.record_success(domain)
                return {"success": True, "job": result}
            return {"success": False, "error": "Lever API failed", "url": url}

        account, shortcode, _ = _parse_workable_url(url)

        result = extract_job_with_dom_stealth(url, account, shortcode)
        if result and result.get("title"):
            result["method"] = "stealth_dom"
            rate_limiter.record_success(domain)
            return {"success": True, "job": result}

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


# ============================================================================
# CHECKPOINT ENDPOINTS
# ============================================================================

@app.get("/checkpoint-status")
def checkpoint_status(url: str = Query(None, description="Specific board URL (omit for all)")):
    """View saved checkpoints to see where the last run stopped."""
    if url:
        cp = get_checkpoint(url)
        if not cp:
            return {"success": True, "url": url, "checkpoint": None, "message": "No checkpoint found"}
        return {"success": True, "url": url, "checkpoint": cp}
    return {"success": True, "checkpoints": list_checkpoints()}


@app.post("/checkpoint-clear")
def checkpoint_clear(url: str = Query(..., description="Board URL to clear checkpoint for")):
    """Clear checkpoint for a board so the next run fetches ALL jobs again."""
    clear_checkpoint(url)
    return {"success": True, "message": f"Checkpoint cleared for {url}"}


# ============================================================================
# RATE LIMIT ENDPOINTS (unchanged)
# ============================================================================

@app.get("/rate-limit-status")
def rate_limit_status(domain: str = Query("apply.workable.com")):
    stats = rate_limiter.get_stats(domain)
    return {"success": True, "stats": stats, "can_proceed": not stats["in_cooldown"]}


@app.post("/reset-rate-limit")
def reset_rate_limit(domain: str = Query(...)):
    if domain in rate_limiter.domain_cooldowns:
        del rate_limiter.domain_cooldowns[domain]
    rate_limiter.consecutive_errors[domain] = 0
    rate_limiter.request_times[domain] = []
    return {"success": True, "message": f"Reset for {domain}"}


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "version": "8.0-checkpoint",
        "bright_data": {
            "enabled": USE_BRIGHT_DATA,
            "host": BD_HOST if USE_BRIGHT_DATA else None,
            "port": BD_PORT if USE_BRIGHT_DATA else None,
        },
        "checkpoint_file": os.path.abspath("scraper_checkpoints.json"),
        "scraping_order": ["stealth_dom", "embedded_jobboard", "api_fallback"],
        "rate_limiter": {
            "max_requests_per_5min": rate_limiter.max_requests_per_5min,
            "min_delay_seconds": rate_limiter.min_delay_seconds,
            "max_delay_seconds": rate_limiter.max_delay_seconds
        }
    }


@app.get("/")
def root():
    return {
        "name": "Workable + Lever Scraper v8 - Checkpoint + Date Filtering",
        "version": "8.0",
        "whats_new": [
            "📅 Flexible date filtering: ?days=5 or ?hours=24",
            "🔖 Checkpoint/resume: remembers last run per board URL",
            "⏹️  Smart pagination stop: halts once date cutoff is reached",
            "🗃️  Checkpoint persisted to scraper_checkpoints.json",
        ],
        "date_filter_priority": [
            "1. hours > 0   →  last N hours (e.g. ?hours=24)",
            "2. days  > 0   →  last N days  (e.g. ?days=5)",
            "3. use_checkpoint=true  →  resume from last saved run",
            "4. no filter   →  fetch everything",
        ],
        "new_endpoints": {
            "/checkpoint-status": "GET ?url=BOARD_URL  (omit url= for all boards)",
            "/checkpoint-clear":  "POST ?url=BOARD_URL",
        },
        "endpoints": {
            "/get-job-links":      "GET ?url=BOARD_URL&days=5&hours=0&use_checkpoint=true&save_progress=true",
            "/get-job-details":    "GET ?url=JOB_URL",
            "/checkpoint-status":  "GET ?url=BOARD_URL",
            "/checkpoint-clear":   "POST ?url=BOARD_URL",
            "/rate-limit-status":  "GET ?domain=DOMAIN",
            "/reset-rate-limit":   "POST ?domain=DOMAIN",
            "/health":             "GET",
        }
    }