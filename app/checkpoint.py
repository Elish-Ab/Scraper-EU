"""
checkpoint.py - Persistent run state for the Workable/Lever scraper.

Stores per-board checkpoint data in a local JSON file so that repeated
runs only fetch NEW jobs instead of re-scraping everything.

Checkpoint file location: ./scraper_checkpoints.json  (configurable via
CHECKPOINT_FILE env var).

Schema per board URL:
{
  "<board_url>": {
    "last_run_at":      "2025-06-01T12:00:00+00:00",   # ISO-8601 UTC
    "last_job_url":     "https://apply.workable.com/acme/j/ABC123/",
    "last_job_title":   "Senior Engineer",
    "last_published":   "2025-06-01",
    "total_jobs_seen":  42,
    "run_count":        7
  }
}
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "scraper_checkpoints.json")


def _load() -> dict:
    path = Path(CHECKPOINT_FILE)
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"[checkpoint] Could not load {CHECKPOINT_FILE}: {e}")
        return {}


def _save(data: dict):
    try:
        path = Path(CHECKPOINT_FILE)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"[checkpoint] Could not save {CHECKPOINT_FILE}: {e}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_checkpoint(board_url: str) -> Optional[dict]:
    """Return the stored checkpoint for *board_url*, or None if not found."""
    data = _load()
    return data.get(board_url.rstrip("/"))


def save_checkpoint(
    board_url: str,
    *,
    last_job_url: str = "",
    last_job_title: str = "",
    last_published: str = "",
    total_jobs_seen: int = 0,
):
    """
    Persist checkpoint info for *board_url*.

    Call this after a successful scrape run so the next run can pick up
    only jobs published AFTER last_published.
    """
    data = _load()
    key = board_url.rstrip("/")
    prev = data.get(key, {})

    data[key] = {
        "last_run_at": datetime.now(timezone.utc).isoformat(),
        "last_job_url": last_job_url,
        "last_job_title": last_job_title,
        "last_published": last_published,
        "total_jobs_seen": total_jobs_seen,
        "run_count": prev.get("run_count", 0) + 1,
    }
    _save(data)
    logger.info(
        f"[checkpoint] ✅ Saved for {key}: "
        f"last_published={last_published!r} "
        f"run_count={data[key]['run_count']}"
    )


def clear_checkpoint(board_url: str):
    """Remove the checkpoint for *board_url* so the next run is a full scan."""
    data = _load()
    key = board_url.rstrip("/")
    if key in data:
        del data[key]
        _save(data)
        logger.info(f"[checkpoint] 🗑️  Cleared checkpoint for {key}")


def list_checkpoints() -> dict:
    """Return all stored checkpoints (for the /checkpoint-status endpoint)."""
    return _load()


# ---------------------------------------------------------------------------
# Date-range helper
# ---------------------------------------------------------------------------

def compute_since_dt(
    days: float = 0,
    hours: float = 0,
    board_url: str = "",
    use_checkpoint: bool = True,
) -> Optional[datetime]:
    """
    Return the earliest publish datetime we care about, in UTC.

    Priority:
      1. Explicit days / hours parameter (if > 0)
      2. Checkpoint's last_published date  (if use_checkpoint=True)
      3. None  → caller fetches everything

    Examples:
      compute_since_dt(days=5)          → now - 5 days
      compute_since_dt(hours=24)        → now - 24 hours
      compute_since_dt(days=0, hours=0) → uses checkpoint or None
    """
    from datetime import timedelta

    if days > 0 or hours > 0:
        delta = timedelta(days=days, hours=hours)
        since = datetime.now(timezone.utc) - delta
        logger.info(f"[checkpoint] 📅 since={since.isoformat()} (explicit days={days} hours={hours})")
        return since

    if use_checkpoint and board_url:
        cp = get_checkpoint(board_url)
        if cp and cp.get("last_published"):
            try:
                # last_published is a date string like "2025-06-01"
                # Use start-of-day so we don't miss jobs posted earlier that day
                since = datetime.fromisoformat(cp["last_published"]).replace(
                    tzinfo=timezone.utc
                )
                logger.info(
                    f"[checkpoint] 📅 Resuming from checkpoint: "
                    f"last_published={cp['last_published']} "
                    f"(last_run={cp.get('last_run_at', 'unknown')})"
                )
                return since
            except Exception as e:
                logger.warning(f"[checkpoint] Could not parse last_published: {e}")

    logger.info("[checkpoint] 📅 No date filter – fetching all jobs")
    return None