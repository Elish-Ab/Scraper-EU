# app/lever_scraper.py
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urljoin
import logging
import random
import time
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]


# ============================================================================
# URL HELPERS
# ============================================================================

def is_lever_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host.endswith("lever.co")


def parse_lever_board_url(board_url: str) -> str:
    parsed = urlparse(board_url)
    host   = parsed.netloc.lower()
    parts  = [p for p in parsed.path.strip("/").split("/") if p]

    if host == "api.lever.co":
        try:
            idx = parts.index("postings")
            return parts[idx + 1]
        except (ValueError, IndexError) as exc:
            raise ValueError(f"Cannot parse Lever API board URL: {board_url}") from exc

    if host.endswith("lever.co") and parts:
        return parts[0]

    raise ValueError(f"Cannot parse Lever board URL: {board_url}")


def parse_lever_job_url(job_url: str):
    parsed = urlparse(job_url)
    parts  = [p for p in parsed.path.strip("/").split("/") if p]

    if parsed.netloc.lower() == "api.lever.co":
        try:
            idx = parts.index("postings")
            return parts[idx + 1], parts[idx + 2]
        except (ValueError, IndexError) as exc:
            raise ValueError(f"Cannot parse Lever API job URL: {job_url}") from exc

    if len(parts) >= 2:
        return parts[0], parts[1]

    raise ValueError(f"Cannot parse Lever job URL: {job_url}")


# ============================================================================
# BOARD PAGE — DOM SCRAPER
# ============================================================================

def _scrape_lever_board_dom(board_url: str) -> list:
    """
    Scrape the Lever board page HTML directly.

    Confirmed DOM structure (Mediafly + Gurobi boards):

    Board page:
      Job container   div.posting[data-qa-posting-id]
      Job ID          data-qa-posting-id attribute
      URL             a.posting-title[href]
      Title           h5[data-qa="posting-name"]
      Department      nearest preceding div.posting-category-title.large-category-label
                      (shared across all jobs in a postings-group)
      Workplace       span.workplaceTypes  (inside div.posting-categories)
      Commitment/type span.commitment
      Location        span.location

    Note: Lever does NOT render a published date in the DOM.
          Date filtering always requires the API (_fetch_lever_api_dates).

    Returns list of job dicts with:
      url, jobId, title, department, workplace, type, location, method
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(board_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Lever board DOM: HTTP {resp.status_code}")
            return []
    except Exception as e:
        logger.warning(f"   Lever board DOM fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    jobs = []

    # Each postings-group has a shared department label above the job listings
    for group in soup.select("div.postings-group"):
        # Department label — shared by all jobs in this group
        dept_el = group.select_one("div.posting-category-title.large-category-label")
        department = dept_el.get_text(strip=True) if dept_el else None

        for posting in group.select("div.posting[data-qa-posting-id]"):
            try:
                job_id = posting.get("data-qa-posting-id", "").strip()
                if not job_id:
                    continue

                # URL from the posting-title anchor
                link_el = posting.select_one("a.posting-title[href]")
                if not link_el:
                    continue
                url = link_el.get("href", "").strip()
                if not url:
                    continue

                # Title
                title_el = posting.select_one("h5[data-qa='posting-name']")
                if not title_el:
                    continue
                title = title_el.get_text(strip=True)
                if not title:
                    continue

                job = {
                    "url":    url,
                    "jobId":  job_id,
                    "title":  title,
                    "method": "stealth_dom",
                }

                if department:
                    job["department"] = department

                # Categories inside the posting-title anchor
                cats = posting.select_one("div.posting-categories")
                if cats:
                    wp = cats.select_one("span.workplaceTypes")
                    if wp:
                        job["workplace"] = wp.get_text(strip=True).replace("\xa0", "").strip(" —")

                    cm = cats.select_one("span.commitment")
                    if cm:
                        job["type"] = cm.get_text(strip=True)

                    loc = cats.select_one("span.location")
                    if loc:
                        raw = loc.get_text(strip=True)
                        # "Remote - US / Remote - Canada" → list
                        parts = [p.strip() for p in raw.split("/") if p.strip()]
                        job["location"]  = parts[0] if parts else raw
                        job["locations"] = parts if len(parts) > 1 else None

                jobs.append(job)

            except Exception as e:
                logger.debug(f"   Lever DOM job parse error: {e}")
                continue

    logger.info(f"   Lever board DOM: {len(jobs)} jobs")
    return jobs


# ============================================================================
# API DATE FETCHER — always needed since DOM has no published date
# ============================================================================

def _fetch_lever_api_dates(company: str) -> dict:
    """
    Fetch all Lever postings from public API and return
    {posting_id: {published, pub_date}} for date filtering.

    Lever's public API returns createdAt (Unix ms timestamp).
    """
    api_url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }
    api_map = {}

    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"   Lever API date fetch: HTTP {resp.status_code}")
            return {}

        for job in resp.json():
            job_id     = job.get("id")
            created_at = job.get("createdAt")
            if job_id and created_at:
                pub_date = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
                api_map[job_id] = {
                    "published": pub_date.date().isoformat(),
                    "pub_date":  pub_date,
                }

        logger.info(f"   Lever API date map: {len(api_map)} jobs")

    except Exception as e:
        logger.warning(f"   Lever API date fetch failed: {e}")

    return api_map


# ============================================================================
# GET LINKS — DOM FIRST, API FALLBACK
# ============================================================================

def get_links_from_lever_api(board_url: str, since_dt: datetime = None, days: int = None) -> list:
    """
    Get Lever job listings.

    Step 1 — DOM scrape board page:
        Extracts title, url, jobId, department, workplace, type, location
        from the board HTML. Fast, no browser needed.

    Step 2 — API date enrichment:
        Lever never renders published dates in DOM.
        Always calls the public Lever API to get createdAt dates.
        Merges published date into each DOM job.

    Step 3 — Date filter:
        Filter by since_dt (preferred) or days fallback.
        Jobs with no API date match → include (don't drop blindly).

    Step 4 — API fallback:
        If DOM scrape fails entirely, fall back to API-only mode
        (returns url + title + countries + published, no DOM metadata).
    """
    try:
        company = parse_lever_board_url(board_url)
    except ValueError as exc:
        logger.error(str(exc))
        return []

    # Resolve since_dt from days fallback
    if since_dt is None and days is not None and days > 0:
        since_dt = datetime.now(timezone.utc) - timedelta(days=days)

    # ── STEP 1: DOM scrape ─────────────────────────────────────────────────
    dom_jobs = _scrape_lever_board_dom(board_url)

    # ── STEP 2: Always fetch API dates (Lever DOM never has published date) ─
    api_date_map = _fetch_lever_api_dates(company)

    if dom_jobs:
        # ── STEP 3: Merge dates + filter ──────────────────────────────────
        filtered = []
        for job in dom_jobs:
            job_id   = job.get("jobId", "")
            api_info = api_date_map.get(job_id)

            if api_info:
                job["published"] = api_info["published"]

                if since_dt:
                    if api_info["pub_date"] < since_dt:
                        continue  # too old

            # No API match → include (job may be very new / unlisted in API)
            filtered.append(job)

        logger.info(f"✅ Lever DOM: {len(filtered)} jobs (after date filter)")
        return filtered

    # ── STEP 4: API-only fallback ──────────────────────────────────────────
    logger.warning(f"   Lever DOM scrape failed → falling back to API-only for '{company}'")
    return _get_links_from_lever_api_only(company, board_url, since_dt, api_date_map)


def _get_links_from_lever_api_only(
    company: str,
    board_url: str,
    since_dt: datetime,
    api_date_map: dict,
) -> list:
    """
    API-only fallback when DOM scrape fails.
    Reuses already-fetched api_date_map to avoid a second API call.
    """
    jobs = []

    for job_id, info in api_date_map.items():
        if since_dt and info["pub_date"] < since_dt:
            continue
        url = f"https://jobs.lever.co/{company}/{job_id}"
        jobs.append({
            "url":       url,
            "jobId":     job_id,
            "published": info["published"],
            "method":    "api",
        })

    # If api_date_map was empty (e.g. both DOM and API failed), try full API
    if not jobs and not api_date_map:
        try:
            api_url = f"https://api.lever.co/v0/postings/{company}?mode=json"
            headers = {"Accept": "application/json", "User-Agent": random.choice(USER_AGENTS)}
            resp    = requests.get(api_url, headers=headers, timeout=20)
            if resp.status_code == 200:
                for job in resp.json():
                    job_id     = job.get("id")
                    created_at = job.get("createdAt")
                    title      = job.get("text") or job.get("title")
                    hosted_url = job.get("hostedUrl") or job.get("applyUrl")
                    if not job_id or not hosted_url:
                        continue

                    pub_date = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc) if created_at else None
                    if since_dt and pub_date and pub_date < since_dt:
                        continue

                    cats      = job.get("categories", {}) if isinstance(job.get("categories"), dict) else {}
                    locations = job.get("locationQuestions") or []
                    country   = job.get("country")

                    jobs.append({
                        "url":       hosted_url,
                        "jobId":     job_id,
                        "title":     title,
                        "published": pub_date.date().isoformat() if pub_date else "",
                        "department": cats.get("team"),
                        "location":  cats.get("location"),
                        "type":      cats.get("commitment"),
                        "workplace": job.get("workplaceType", ""),
                        "remote":    (job.get("workplaceType") or "").lower() == "remote",
                        "countries": [country] if country else [],
                        "method":    "api",
                    })
        except Exception as e:
            logger.error(f"Lever API-only fallback error: {e}")

    logger.info(f"✅ Lever API fallback: {len(jobs)} jobs")
    return jobs


# ============================================================================
# JOB DETAIL — DOM FIRST, API FALLBACK
# ============================================================================

def extract_job_with_lever_api(job_url: str) -> dict | None:
    """
    Extract full Lever job details.

    Step 1 — DOM scrape detail page:
        Confirmed selectors (Mediafly + Gurobi):
          title           h2 inside div.posting-headline
          location        div.location inside div.posting-categories
          department      div.department
          type            div.commitment
          workplace       div.workplaceTypes
          description     div[data-qa="job-description"]
          requirements    div[data-qa="posting-requirements"] (multiple sections)
          closing/benefits div[data-qa="closing-description"]
          salary range    div[data-qa="salary-range"]
          apply_url       a[data-qa="show-page-apply"]

    Step 2 — API enrichment:
        Gets createdAt (published date), country, and any fields
        missing from the DOM.

    Step 3 — API-only fallback:
        If DOM scrape fails, use API + HTML fallback (original behaviour).
    """
    try:
        company, posting_id = parse_lever_job_url(job_url)
    except ValueError as exc:
        logger.error(str(exc))
        return None

    # ── STEP 1: DOM scrape detail page ────────────────────────────────────
    dom_result = _scrape_lever_detail_dom(job_url, company, posting_id)

    # ── STEP 2: API enrichment (published date, country, fill gaps) ───────
    api_result = _fetch_lever_job_api(company, posting_id, job_url)

    if dom_result:
        if api_result:
            # Merge: API fills in published + country + any missing fields
            dom_result["published"] = api_result.get("published", "")
            dom_result["remote"]    = api_result.get("remote", False)
            if not dom_result.get("country"):
                dom_result["country"] = api_result.get("country")
            if not dom_result.get("location") and api_result.get("location"):
                dom_result["location"] = api_result["location"]
            if not dom_result.get("department") and api_result.get("department"):
                dom_result["department"] = api_result["department"]
            if not dom_result.get("type") and api_result.get("type"):
                dom_result["type"] = api_result["type"]
        dom_result["method"] = "stealth_dom"
        return dom_result

    # ── STEP 3: API-only fallback ──────────────────────────────────────────
    logger.warning(f"   Lever detail DOM failed → using API for {job_url}")
    if api_result:
        api_result["method"] = "api"
        return api_result

    return None


def _scrape_lever_detail_dom(job_url: str, company: str, posting_id: str) -> dict | None:
    """
    Scrape Lever job detail page HTML.

    Confirmed selectors (Mediafly + Gurobi detail pages):
      Title           div.posting-headline h2
      Location        div.posting-categories div.location
      Department      div.posting-categories div.department
      Type/commitment div.posting-categories div.commitment
      Workplace       div.posting-categories div.workplaceTypes
      Description     div[data-qa="job-description"]
      Requirements    div[data-qa="posting-requirements"]  (can be multiple)
      Closing/benefits div[data-qa="closing-description"]
      Salary          div[data-qa="salary-range"]
      Apply URL       a[data-qa="show-page-apply"]
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(job_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Lever detail DOM: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   Lever detail DOM fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Title ──────────────────────────────────────────────────────────────
    title = None
    headline = soup.select_one("div.posting-headline h2")
    if headline:
        title = headline.get_text(strip=True)
    if not title and soup.title:
        title = soup.title.get_text(strip=True)
    if not title:
        return None

    job = {
        "jobId":   posting_id,
        "url":     job_url,
        "account": company,
        "title":   title,
    }

    # ── Location / Department / Type / Workplace from header categories ────
    # Detail page uses div (not span like board page)
    header_cats = soup.select_one("div.posting-headline div.posting-categories")
    if header_cats:
        loc = header_cats.select_one("div.location")
        if loc:
            raw = loc.get_text(strip=True).rstrip(" /").strip()
            if raw:
                parts = [p.strip() for p in raw.split("/") if p.strip()]
                job["location"]  = parts[0] if parts else raw
                if len(parts) > 1:
                    job["locations"] = parts

        dept = header_cats.select_one("div.department")
        if dept:
            raw = dept.get_text(strip=True).rstrip(" /").strip()
            # "Engineering /" → "Engineering"
            job["department"] = raw.rstrip("/").strip()

        cm = header_cats.select_one("div.commitment")
        if cm:
            job["type"] = cm.get_text(strip=True).rstrip(" /").strip()

        wp = header_cats.select_one("div.workplaceTypes")
        if wp:
            job["workplace"] = wp.get_text(strip=True).rstrip(" /").strip()

    # ── Description ────────────────────────────────────────────────────────
    desc_el = soup.select_one('[data-qa="job-description"]')
    if desc_el:
        html = str(desc_el)
        if len(html) > 100:
            job["description"] = html

    # ── Requirements — collect ALL sections (there can be multiple) ────────
    req_sections = soup.select('[data-qa="posting-requirements"]')
    if req_sections:
        # Capture each section with its preceding h3 heading
        reqs_html_parts = []
        for req in req_sections:
            # grab the h3 sibling immediately before
            heading = req.find_previous_sibling("h3") or req.find_previous("h3")
            if heading:
                reqs_html_parts.append(f"<h3>{heading.get_text(strip=True)}</h3>")
            reqs_html_parts.append(str(req))
        combined = "\n".join(reqs_html_parts)
        if len(combined) > 50:
            job["requirements"] = combined

    # ── Closing / Benefits ─────────────────────────────────────────────────
    closing_el = soup.select_one('[data-qa="closing-description"]')
    if closing_el:
        html = str(closing_el)
        if len(html) > 50:
            job["benefits"] = html

    # ── Salary range ───────────────────────────────────────────────────────
    salary_el = soup.select_one('[data-qa="salary-range"]')
    if salary_el:
        salary_text = salary_el.get_text(" ", strip=True)
        if salary_text:
            job["salary_range"] = salary_text

    # ── Apply URL ──────────────────────────────────────────────────────────
    apply_el = soup.select_one('a[data-qa="show-page-apply"]')
    if apply_el:
        apply_href = apply_el.get("href", "")
        if apply_href:
            job["apply_url"] = apply_href

    has_content = bool(job.get("description") or job.get("requirements") or job.get("location"))
    return job if has_content else None


def _fetch_lever_job_api(company: str, posting_id: str, job_url: str) -> dict | None:
    """
    Fetch single job from Lever public API.
    Primary use: get published date and country to enrich DOM result.
    Also used as full fallback if DOM fails.
    """
    api_url = f"https://api.lever.co/v0/postings/{company}/{posting_id}?mode=json"
    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"   Lever job API: HTTP {resp.status_code}")
            return None

        data       = resp.json()
        created_at = data.get("createdAt")
        created_dt = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc) if created_at else None
        cats       = data.get("categories", {}) if isinstance(data.get("categories"), dict) else {}
        wp_type    = data.get("workplaceType") or ""

        return {
            "jobId":            data.get("id") or posting_id,
            "url":              data.get("hostedUrl") or job_url,
            "account":          company,
            "title":            data.get("text"),
            "department":       cats.get("team"),
            "published":        created_dt.isoformat() if created_dt else "",
            "location":         cats.get("location"),
            "type":             cats.get("commitment"),
            "workplace":        wp_type,
            "remote":           wp_type.lower() == "remote",
            "country":          data.get("country"),
            "description":      data.get("description"),
            "descriptionPlain": data.get("descriptionPlain"),
            "lists":            data.get("lists"),
        }

    except Exception as e:
        logger.error(f"   Lever job API error: {e}")
        return None