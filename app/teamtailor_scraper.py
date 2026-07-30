# app/teamtailor_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
import json
import logging
import random
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

TEAMTAILOR_SUFFIXES = (".teamtailor.com",)


# ============================================================================
# URL HELPERS
# ============================================================================

def is_teamtailor_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return any(host.endswith(s) for s in TEAMTAILOR_SUFFIXES)


def _board_feed_url(board_url: str) -> str:
    """Return the /jobs.json feed URL for a TeamTailor board."""
    p = urlparse(board_url)
    return f"{p.scheme}://{p.netloc}/jobs.json"


def _job_id_from_url(url: str) -> str | None:
    """Extract numeric job ID from /jobs/8011703-some-slug."""
    parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
    for i, part in enumerate(parts):
        if part == "jobs" and i + 1 < len(parts):
            slug = parts[i + 1].split("?")[0]
            numeric = slug.split("-")[0]
            if numeric.isdigit():
                return numeric
    return None


def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def _parse_date(date_str: str) -> str | None:
    """Normalize ISO 8601 date to YYYY-MM-DD."""
    if not date_str:
        return None
    try:
        if date_str.endswith("Z"):
            date_str = date_str[:-1] + "+00:00"
        return datetime.fromisoformat(date_str).date().isoformat()
    except Exception:
        return date_str[:10] if len(date_str) >= 10 else None


# ============================================================================
# GET JOB LINKS — /jobs.json JSON Feed (SSR, no JS required)
# ============================================================================

def get_links_from_teamtailor(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch TeamTailor job board via the /jobs.json JSON Feed endpoint.

    Every TeamTailor board exposes /jobs.json (JSON Feed 1.1 spec).
    Each item: id (UUID), title, url, date_published, content_html,
    _jobposting (schema.org JobPosting with datePosted, employmentType,
    jobLocation, jobLocationType, applicantLocationRequirements).

    No pagination — the feed returns all active jobs in one request.
    """
    feed_url = _board_feed_url(board_url)
    try:
        resp = requests.get(
            feed_url,
            headers={**_headers(), "Accept": "application/json"},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning(f"   TeamTailor feed: HTTP {resp.status_code} for {feed_url}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"   TeamTailor feed fetch failed: {e}")
        return []

    items = data.get("items", [])
    jobs: list[dict] = []
    seen_ids: set[str] = set()

    for item in items:
        job_url = (item.get("url") or "").strip()
        if not job_url:
            continue

        job_id = _job_id_from_url(job_url) or (item.get("id") or "").strip()
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title = (item.get("title") or "").strip()
        if not title:
            continue

        date_published = _parse_date(item.get("date_published", ""))

        # Apply since_dt filter
        if since_dt and date_published:
            try:
                pub_dt = datetime.fromisoformat(date_published).replace(tzinfo=timezone.utc)
                if pub_dt < since_dt:
                    continue
            except Exception:
                pass

        job: dict = {
            "jobId": job_id,
            "url": job_url,
            "title": title,
            "method": "teamtailor_feed",
        }

        if date_published:
            job["posted"] = date_published

        # Pull metadata from the embedded _jobposting schema.org block
        posting = item.get("_jobposting") or {}
        emp_type = (posting.get("employmentType") or "").strip()
        if emp_type:
            job["type"] = emp_type

        loc_type = (posting.get("jobLocationType") or "").strip()
        if loc_type == "TELECOMMUTE":
            job["workplace"] = "Remote"

        loc_reqs = posting.get("applicantLocationRequirements") or []
        countries = [
            r.get("name", "") for r in loc_reqs
            if isinstance(r, dict) and r.get("name")
        ]
        if countries:
            job["eligible_countries"] = countries

        job_locs = posting.get("jobLocation") or []
        if job_locs and isinstance(job_locs, list):
            addr = (job_locs[0].get("address") or {})
            city    = (addr.get("addressLocality") or "").strip()
            country = (addr.get("addressCountry") or "").strip()
            if city and city != country:
                job["location"] = city
            elif country:
                job["location"] = country

        # Description from content_html
        content = (item.get("content_html") or "").strip()
        if len(content) > 50:
            job["description"] = content

        jobs.append(job)

    logger.info(f"✅ TeamTailor: {len(jobs)} jobs from {feed_url}")
    return jobs


# ============================================================================
# JOB DETAIL — SSR HTML page with LD+JSON
# ============================================================================

def extract_job_with_teamtailor(job_url: str) -> dict | None:
    """
    Scrape a TeamTailor job detail page (server-side rendered).

    Primary source: <script type="application/ld+json"> (schema.org JobPosting)
      datePosted, employmentType, jobLocationType, jobLocation,
      applicantLocationRequirements, hiringOrganization.name
    Supplemental: visible metadata spans (department, location, workplace)
    Description: content of .job-description or a[data-variabletextwrapper]
    Apply URL: constructed as job_url + /apply (TeamTailor standard)
    """
    job_id = _job_id_from_url(job_url)

    try:
        resp = requests.get(job_url, headers=_headers(), timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   TeamTailor job: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   TeamTailor job fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Title ──────────────────────────────────────────────────────────────
    h1 = soup.select_one("h1")
    if not h1:
        return None
    title = h1.get_text(strip=True)
    if not title:
        return None

    job: dict = {
        "jobId": job_id or job_url.split("/jobs/")[-1].split("?")[0],
        "url": job_url,
        "title": title,
    }

    # ── LD+JSON ────────────────────────────────────────────────────────────
    ld_tag = soup.find("script", type="application/ld+json")
    if ld_tag and ld_tag.string:
        try:
            ld = json.loads(ld_tag.string)
            date_posted = _parse_date(ld.get("datePosted", ""))
            if date_posted:
                job["posted"] = date_posted

            emp_type = (ld.get("employmentType") or "").strip()
            if emp_type:
                job["type"] = emp_type

            if ld.get("jobLocationType") == "TELECOMMUTE":
                job["workplace"] = "Remote"

            loc_reqs = ld.get("applicantLocationRequirements") or []
            countries = [
                r.get("name", "") for r in loc_reqs
                if isinstance(r, dict) and r.get("name")
            ]
            if countries:
                job["eligible_countries"] = countries

            job_locs = ld.get("jobLocation") or []
            if job_locs and isinstance(job_locs, list):
                addr = (job_locs[0].get("address") or {})
                city    = (addr.get("addressLocality") or "").strip()
                country = (addr.get("addressCountry") or "").strip()
                if city and city != country:
                    job["location"] = city
                elif country:
                    job["location"] = country

            org = ld.get("hiringOrganization") or {}
            company = (org.get("name") or "").strip()
            if company:
                job["company"] = company
        except Exception as e:
            logger.debug(f"   TeamTailor LD+JSON parse error: {e}")

    # ── Company website ─────────────────────────────────────────────────────
    # LD+JSON hiringOrganization.sameAs just points back to the TeamTailor-
    # hosted board itself, not the real company site. The genuine site is
    # linked from the header/footer, labeled "Homepage".
    homepage_span = soup.find("span", string=lambda s: bool(s and s.strip() == "Homepage"))
    if homepage_span:
        link = homepage_span.find_next("a", href=True)
        if link:
            href = link["href"].strip()
            if href.startswith("http"):
                job["company_website"] = href

    # ── Metadata spans (department, visible location, workplace) ──────────
    # The first metadata div has: dept · location · workplace
    meta_spans = [
        s.get_text(strip=True)
        for s in soup.select("[data-variabletextwrapper] .text-sm span, [data-variabletextwrapper] div.text-sm span")
        if s.get_text(strip=True) and s.get_text(strip=True) != "·"
    ]
    # de-dupe while preserving order (the div appears twice in DOM)
    seen = set()
    clean_spans = []
    for s in meta_spans:
        if s not in seen:
            seen.add(s)
            clean_spans.append(s)

    if clean_spans:
        job["department"] = clean_spans[0]
        if not job.get("location") and len(clean_spans) > 1:
            job["location"] = clean_spans[1]
        if not job.get("workplace") and len(clean_spans) > 2:
            job["workplace"] = clean_spans[2]

    # ── Description ────────────────────────────────────────────────────────
    # TeamTailor renders the prose body in a [class*="prose"] div (SSR)
    desc_el = soup.select_one("[class*='prose']")
    if not desc_el:
        # Fall back: strip header from main wrapper
        wrapper = soup.select_one("[data-variabletextwrapper]")
        if wrapper:
            for tag in wrapper.select("h1, h2, button, p.mt-4"):
                tag.decompose()
            desc_el = wrapper

    if desc_el:
        for tag in desc_el.select("script, style, svg"):
            tag.decompose()
        html = str(desc_el).strip()
        if len(html) > 100:
            job["description"] = html

    # ── Apply URL ──────────────────────────────────────────────────────────
    job["apply_url"] = job_url.rstrip("/") + "/apply"

    has_content = bool(job.get("description") or job.get("location") or job.get("department"))
    return job if has_content else None
