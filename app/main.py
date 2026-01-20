# app/main.py - Modified for n8n workflow integration
from fastapi import FastAPI, Query, HTTPException #type: ignore
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout #type: ignore
import requests
from datetime import datetime, timedelta, timezone
import re
import logging
import json
import time
import random
from collections import defaultdict
from typing import Optional
from app.lever_scraper import extract_job_with_lever_api, is_lever_url

app = FastAPI()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2

# ============================================================================
# GLOBAL RATE LIMITER - Persists across n8n requests
# ============================================================================

class GlobalRateLimiter:
    """
    Rate limiter that works across multiple n8n requests
    Tracks requests globally to prevent n8n from overwhelming the API
    """
    
    def __init__(self):
        # Track requests per domain
        self.request_times = defaultdict(list)
        self.domain_cooldowns = {}
        self.consecutive_errors = defaultdict(int)
        
        # Configuration - VERY CONSERVATIVE for n8n
        self.max_requests_per_5min = 8
        self.min_delay_seconds = 5
        self.max_delay_seconds = 10
        self.cooldown_minutes = 15
        
    def can_make_request(self, domain: str) -> tuple[bool, float]:
        """
        Check if we can make a request now
        Returns: (can_proceed, wait_seconds)
        """
        now = datetime.now()
        
        # Check if domain is in cooldown
        if domain in self.domain_cooldowns:
            cooldown_end = self.domain_cooldowns[domain]
            if now < cooldown_end:
                wait_seconds = (cooldown_end - now).total_seconds()
                logger.warning(f"⏸️  Domain {domain} in cooldown: {wait_seconds:.0f}s remaining")
                return False, wait_seconds
            else:
                # Cooldown expired
                del self.domain_cooldowns[domain]
                self.consecutive_errors[domain] = 0
        
        # Clean old requests (older than 5 minutes)
        cutoff = now - timedelta(minutes=5)
        self.request_times[domain] = [
            t for t in self.request_times[domain] if t > cutoff
        ]
        
        # Check if we've hit the rate limit
        recent_requests = len(self.request_times[domain])
        
        if recent_requests >= self.max_requests_per_5min:
            # Calculate wait time until oldest request expires
            oldest = self.request_times[domain][0]
            wait_time = 300 - (now - oldest).total_seconds()
            
            if wait_time > 0:
                logger.warning(f"⏳ Rate limit reached: {recent_requests}/{self.max_requests_per_5min} requests in 5min")
                return False, wait_time
        
        # Add minimum delay between requests
        if self.request_times[domain]:
            last_request = self.request_times[domain][-1]
            time_since_last = (now - last_request).total_seconds()
            
            if time_since_last < self.min_delay_seconds:
                wait_needed = self.min_delay_seconds - time_since_last
                logger.info(f"⏱️  Enforcing minimum delay: {wait_needed:.1f}s")
                return False, wait_needed
        
        return True, 0
    
    def record_request(self, domain: str):
        """Record that a request was made"""
        self.request_times[domain].append(datetime.now())
    
    def record_success(self, domain: str):
        """Record successful request"""
        if domain in self.consecutive_errors:
            self.consecutive_errors[domain] = max(0, self.consecutive_errors[domain] - 1)
    
    def record_rate_limit(self, domain: str):
        """Record that we got rate limited"""
        self.consecutive_errors[domain] += 1
        
        # Increase cooldown time based on consecutive errors
        cooldown_multiplier = min(self.consecutive_errors[domain], 4)
        cooldown_minutes = self.cooldown_minutes * cooldown_multiplier
        
        self.domain_cooldowns[domain] = datetime.now() + timedelta(minutes=cooldown_minutes)
        
        logger.error(f"🚫 Rate limited! Cooldown: {cooldown_minutes} min (error #{self.consecutive_errors[domain]})")
    
    def get_stats(self, domain: str) -> dict:
        """Get current stats for a domain"""
        now = datetime.now()
        cutoff = now - timedelta(minutes=5)
        
        recent_requests = [t for t in self.request_times[domain] if t > cutoff]
        
        in_cooldown = False
        cooldown_remaining = 0
        
        if domain in self.domain_cooldowns:
            cooldown_end = self.domain_cooldowns[domain]
            if now < cooldown_end:
                in_cooldown = True
                cooldown_remaining = (cooldown_end - now).total_seconds()
        
        return {
            "domain": domain,
            "requests_last_5min": len(recent_requests),
            "max_requests_per_5min": self.max_requests_per_5min,
            "in_cooldown": in_cooldown,
            "cooldown_remaining_seconds": int(cooldown_remaining),
            "consecutive_errors": self.consecutive_errors[domain]
        }

# Global instance - persists across requests
global_rate_limiter = GlobalRateLimiter()


class BrowserRotator:
    """Rotate browser fingerprints"""
    
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    ]
    
    @classmethod
    def get_headers(cls):
        return {
            'User-Agent': random.choice(cls.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': random.choice(['en-US,en;q=0.9', 'en-GB,en;q=0.9', 'en-CA,en;q=0.9']),
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    @classmethod
    def get_random_viewport(cls):
        return {
            'width': random.choice([1366, 1440, 1920]),
            'height': random.choice([768, 900, 1080])
        }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def wait_for_rate_limit(domain: str, max_wait: int = 300):
    """
    Wait if needed before making request
    Returns True if request can proceed, False if should abort
    """
    can_proceed, wait_seconds = global_rate_limiter.can_make_request(domain)
    
    if not can_proceed:
        if wait_seconds > max_wait:
            # Wait time too long - tell n8n to retry later
            logger.error(f"❌ Wait time too long: {wait_seconds:.0f}s (max: {max_wait}s)")
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "rate_limit_exceeded",
                    "message": f"Please retry after {int(wait_seconds)} seconds",
                    "retry_after_seconds": int(wait_seconds),
                    "domain": domain
                }
            )
        
        # Wait and continue
        logger.info(f"⏳ Waiting {wait_seconds:.0f}s before request...")
        time.sleep(wait_seconds)
        
        # Add random extra delay
        extra_delay = random.uniform(2, 5)
        time.sleep(extra_delay)
    else:
        # Add standard random delay
        delay = random.uniform(
            global_rate_limiter.min_delay_seconds, 
            global_rate_limiter.max_delay_seconds
        )
        logger.info(f"⏱️  Random delay: {delay:.1f}s")
        time.sleep(delay)
    
    # Record the request
    global_rate_limiter.record_request(domain)
    return True


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
# SCRAPING FUNCTIONS
# ============================================================================

def extract_job_with_dom(job_url: str, account: str, shortcode: str):
    """Extract job details from DOM with rate limiting"""
    domain = urlparse(job_url).netloc
    
    for attempt in range(MAX_RETRIES):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled']
                )
                
                context = browser.new_context(
                    user_agent=random.choice(BrowserRotator.USER_AGENTS),
                    viewport=BrowserRotator.get_random_viewport()
                )
                
                page = context.new_page()
                page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(random.randint(3000, 5000))
                
                # Check for error page
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
                for sel in ['[data-ui="job-description"]', 'div[itemprop="description"]', 
                           'section[data-ui="job-description"]', 'div.description']:
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
                        content_selectors = ['main', 'article', '[role="main"]']
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
                for sel in ['[data-ui="job-requirements"]', 'section:has-text("Requirements")']:
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
                for sel in ['[data-ui="job-benefits"]', 'section:has-text("Benefits")']:
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
                    global_rate_limiter.record_success(domain)
                    return job_data
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                        continue
                    return job_data if title else None
                    
        except Exception as e:
            logger.error(f"DOM extraction attempt {attempt + 1} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY_BASE * (2 ** attempt))
                continue
            return None


def extract_job_with_api(account: str, shortcode: str, job_url: str):
    """Extract job from API with rate limiting"""
    endpoints = {
        "v3": f"https://apply.workable.com/api/v3/accounts/{account}/jobs/{shortcode}",
        "v2": f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{shortcode}",
    }
    
    domain = urlparse(job_url).netloc
    headers = BrowserRotator.get_headers()
    headers.update({
        "Accept": "application/json",
        "Content-Type": "application/json",
    })
    
    for name, endpoint in endpoints.items():
        for method in ["POST", "GET"]:
            try:
                if method == "POST":
                    resp = requests.post(endpoint, json={}, headers=headers, timeout=15)
                else:
                    resp = requests.get(endpoint, headers=headers, timeout=15)
                
                if resp.status_code == 429:
                    logger.warning(f"API rate limited (429)")
                    global_rate_limiter.record_rate_limit(domain)
                    raise HTTPException(
                        status_code=429,
                        detail={
                            "error": "rate_limit_exceeded",
                            "message": "Rate limited by target server",
                            "retry_after_seconds": 900,  # 15 minutes
                            "domain": domain
                        }
                    )
                
                if resp.status_code == 200:
                    d = resp.json()
                    global_rate_limiter.record_success(domain)
                    
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
                    
            except HTTPException:
                raise  # Re-raise HTTP exceptions
            except Exception as e:
                logger.debug(f"API {name} {method} failed: {e}")
                continue
    
    return None


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.get("/get-job-details")
def get_job_details(url: str = Query(..., description="Workable or Lever job URL")):
    """
    Get full job details - OPTIMIZED FOR N8N
    
    This endpoint handles rate limiting automatically and returns 429 errors
    when n8n should retry later (n8n will handle the retry automatically)
    """
    logger.info(f"\n🎯 GET DETAILS: {url}")
    
    try:
        domain = urlparse(url).netloc
        
        # CRITICAL: Wait for rate limit before processing
        wait_for_rate_limit(domain, max_wait=300)
        
        # Check if Lever URL
        if is_lever_url(url):
            api_result = extract_job_with_lever_api(url)
            if api_result and api_result.get("title"):
                api_result["method"] = "api"
                global_rate_limiter.record_success(domain)
                return {"success": True, "job": api_result}
            return {"success": False, "error": "Lever API failed", "url": url}
        
        # Parse Workable URL
        account, shortcode = _parse_workable_url(url)
        
        # Try DOM first
        dom_result = extract_job_with_dom(url, account, shortcode)
        if dom_result and dom_result.get("title"):
            dom_result["method"] = "dom"
            return {"success": True, "job": dom_result}
        
        # Try API
        time.sleep(random.uniform(2, 4))
        api_result = extract_job_with_api(account, shortcode, url)
        if api_result and api_result.get("title"):
            api_result["method"] = "api"
            return {"success": True, "job": api_result}
        
        # Final DOM attempt
        time.sleep(random.uniform(3, 5))
        final_result = extract_job_with_dom(url, account, shortcode)
        if final_result and final_result.get("title"):
            final_result["method"] = "dom_final"
            return {"success": True, "job": final_result}
        
        return {"success": False, "error": "All methods failed", "url": url}
        
    except HTTPException:
        raise  # Let FastAPI handle 429 errors
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"success": False, "error": str(e), "url": url}


@app.get("/rate-limit-status")
def rate_limit_status(domain: str = Query("apply.workable.com", description="Domain to check")):
    """
    Check current rate limit status for a domain
    Useful for n8n to check before starting a batch
    """
    stats = global_rate_limiter.get_stats(domain)
    return {
        "success": True,
        "stats": stats,
        "can_proceed": not stats["in_cooldown"]
    }


@app.post("/reset-rate-limit")
def reset_rate_limit(domain: str = Query(..., description="Domain to reset")):
    """
    Reset rate limit counters for a domain
    Use this if you want to manually clear cooldowns
    """
    if domain in global_rate_limiter.domain_cooldowns:
        del global_rate_limiter.domain_cooldowns[domain]
    
    global_rate_limiter.consecutive_errors[domain] = 0
    global_rate_limiter.request_times[domain] = []
    
    return {
        "success": True,
        "message": f"Rate limit reset for {domain}"
    }


@app.get("/health")
def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "version": "7.0-n8n-optimized",
        "rate_limiter": {
            "max_requests_per_5min": global_rate_limiter.max_requests_per_5min,
            "min_delay_seconds": global_rate_limiter.min_delay_seconds,
            "max_delay_seconds": global_rate_limiter.max_delay_seconds,
            "cooldown_minutes": global_rate_limiter.cooldown_minutes
        }
    }


@app.get("/")
def root():
    """API info"""
    return {
        "name": "Workable + Lever Scraper - N8N Optimized",
        "version": "7.0",
        "optimizations": [
            "🔄 Global rate limiter (persists across n8n requests)",
            "⏱️  5-10 second delays between requests",
            "🛡️  Max 8 requests per 5 minutes",
            "🚫 Auto 15-minute cooldowns on rate limits",
            "⚠️  Returns 429 errors for n8n to handle retries",
            "📊 Rate limit status endpoint",
            "🎭 Browser fingerprint rotation"
        ],
        "for_n8n": {
            "main_endpoint": "/get-job-details?url=JOB_URL",
            "check_status": "/rate-limit-status?domain=apply.workable.com",
            "handle_429": "n8n should wait and retry based on retry_after_seconds",
            "recommended_workflow": "Add 'Wait' node in n8n between HTTP requests"
        },
        "endpoints": {
            "/get-job-details": "GET ?url=JOB_URL → Full job details",
            "/rate-limit-status": "GET ?domain=DOMAIN → Check rate limit status",
            "/reset-rate-limit": "POST ?domain=DOMAIN → Reset rate limits",
            "/health": "GET → Health check"
        }
    }