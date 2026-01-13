# app/lever_scraper.py
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import logging
import requests

logger = logging.getLogger(__name__)


def is_lever_url(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    return host.endswith("lever.co")


def parse_lever_board_url(board_url: str) -> str:
    parsed = urlparse(board_url)
    host = parsed.netloc.lower()
    parts = [p for p in parsed.path.strip("/").split("/") if p]

    if host == "api.lever.co":
        try:
            postings_index = parts.index("postings")
            return parts[postings_index + 1]
        except (ValueError, IndexError) as exc:
            raise ValueError(f"Cannot parse Lever API board URL: {board_url}") from exc

    if host.endswith("lever.co") and parts:
        return parts[0]

    raise ValueError(f"Cannot parse Lever board URL: {board_url}")


def parse_lever_job_url(job_url: str):
    parsed = urlparse(job_url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if parsed.netloc.lower() == "api.lever.co":
        try:
            postings_index = parts.index("postings")
            return parts[postings_index + 1], parts[postings_index + 2]
        except (ValueError, IndexError) as exc:
            raise ValueError(f"Cannot parse Lever API job URL: {job_url}") from exc

    if len(parts) >= 2:
        return parts[0], parts[1]

    raise ValueError(f"Cannot parse Lever job URL: {job_url}")


def get_links_from_lever_api(board_url: str, days: int = 5):
    """Get Lever jobs from public API."""
    try:
        company = parse_lever_board_url(board_url)
    except ValueError as exc:
        logger.error(str(exc))
        return []

    api_url = f"https://api.lever.co/v0/postings/{company}?mode=json"
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"Lever API failed: {resp.status_code}")
            return []

        results = resp.json()
        jobs = []
        for job in results:
            created_at = job.get("createdAt")
            if created_at:
                created_dt = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
                if created_dt < cutoff_date:
                    continue
            else:
                created_dt = None

            country = job.get("country")
            countries = [country] if country else []
            workplace_type = job.get("workplaceType") or ""
            hosted_url = job.get("hostedUrl") or job.get("applyUrl")
            title = job.get("text") or job.get("title")
            if not hosted_url or not title:
                continue

            jobs.append({
                "url": hosted_url,
                "title": title,
                "countries": countries,
                "country_count": len(countries),
                "remote": workplace_type.lower() == "remote",
                "published": created_dt.date().isoformat() if created_dt else "",
                "method": "api",
            })

        logger.info(f"✅ Lever API: {len(jobs)} jobs")
        return jobs
    except Exception as exc:
        logger.error(f"Lever API error: {exc}")
        return []


def extract_job_with_lever_api(job_url: str):
    """Extract Lever job details from API."""
    try:
        company, posting_id = parse_lever_job_url(job_url)
    except ValueError as exc:
        logger.error(str(exc))
        return None

    api_url = f"https://api.lever.co/v0/postings/{company}/{posting_id}?mode=json"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"Lever API failed: {resp.status_code}")
            return None

        data = resp.json()
        created_at = data.get("createdAt")
        created_dt = (
            datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
            if created_at
            else None
        )
        categories = data.get("categories", {}) if isinstance(data.get("categories"), dict) else {}
        workplace_type = data.get("workplaceType") or ""

        return {
            "jobId": data.get("id") or posting_id,
            "url": data.get("hostedUrl") or job_url,
            "account": company,
            "title": data.get("text"),
            "department": categories.get("team"),
            "published": created_dt.isoformat() if created_dt else "",
            "location": categories.get("location"),
            "type": categories.get("commitment"),
            "workplace": workplace_type,
            "remote": workplace_type.lower() == "remote",
            "country": data.get("country"),
            "description": data.get("description"),
            "descriptionPlain": data.get("descriptionPlain"),
            "opening": data.get("opening"),
            "openingPlain": data.get("openingPlain"),
            "descriptionBody": data.get("descriptionBody"),
            "descriptionBodyPlain": data.get("descriptionBodyPlain"),
            "lists": data.get("lists"),
        }
    except Exception as exc:
        logger.error(f"Lever API error: {exc}")
        return None
