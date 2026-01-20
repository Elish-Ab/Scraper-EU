# app/rate_limiter.py - Smart Rate Limiting System

import time
import random
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)


class SmartRateLimiter:
    """Prevents blocks by mimicking human behavior and enforcing safe limits"""
    
    def __init__(self, 
                 requests_per_5min=8,      # Conservative: 8 requests per 5 minutes
                 min_delay=4,               # Minimum 4 seconds between requests
                 max_delay=10,              # Maximum 10 seconds between requests
                 cooldown_after_error=15):  # 15 minute cooldown after rate limit
        
        self.requests_per_5min = requests_per_5min
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.cooldown_minutes = cooldown_after_error
        
        self.request_times = defaultdict(list)
        self.domain_cooldowns = {}
        self.error_counts = defaultdict(int)
        
    def wait_before_request(self, domain: str):
        """Enforce safe rate limits per domain"""
        now = datetime.now()
        
        # 1. Check if domain is in cooldown
        if domain in self.domain_cooldowns:
            cooldown_end = self.domain_cooldowns[domain]
            if now < cooldown_end:
                wait_seconds = (cooldown_end - now).total_seconds()
                logger.warning(f"⏸️  [{domain}] In cooldown: waiting {wait_seconds:.0f}s")
                time.sleep(wait_seconds)
                del self.domain_cooldowns[domain]
        
        # 2. Remove requests older than 5 minutes
        cutoff = now - timedelta(minutes=5)
        self.request_times[domain] = [
            t for t in self.request_times[domain] if t > cutoff
        ]
        
        # 3. Check recent request count
        recent_requests = len(self.request_times[domain])
        
        if recent_requests >= self.requests_per_5min:
            oldest_request = self.request_times[domain][0]
            wait_time = 300 - (now - oldest_request).total_seconds()
            
            if wait_time > 0:
                buffer = random.uniform(5, 15)
                total_wait = wait_time + buffer
                logger.info(f"⏳ [{domain}] Rate limit: {recent_requests}/{self.requests_per_5min}. Waiting {total_wait:.0f}s")
                time.sleep(total_wait)
                
                now = datetime.now()
                cutoff = now - timedelta(minutes=5)
                self.request_times[domain] = [
                    t for t in self.request_times[domain] if t > cutoff
                ]
        
        # 4. Random delay (human-like behavior)
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug(f"⏱️  [{domain}] Delay: {delay:.1f}s")
        time.sleep(delay)
        
        # 5. Record request
        self.request_times[domain].append(now)
        
    def trigger_cooldown(self, domain: str, minutes: Optional[int] = None):
        """Put domain on cooldown after errors"""
        if minutes is None:
            minutes = self.cooldown_minutes
            
        self.domain_cooldowns[domain] = datetime.now() + timedelta(minutes=minutes)
        self.error_counts[domain] += 1
        
        logger.warning(f"🚫 [{domain}] Cooldown: {minutes} min (error #{self.error_counts[domain]})")
    
    def reset_domain(self, domain: str):
        """Reset error count after successful requests"""
        if domain in self.error_counts and self.error_counts[domain] > 0:
            self.error_counts[domain] = max(0, self.error_counts[domain] - 1)


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
        """Generate realistic headers"""
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
        """Get random viewport"""
        return {
            'width': random.choice([1366, 1440, 1920]),
            'height': random.choice([768, 900, 1080])
        }


class SessionManager:
    """Manage sessions with breaks"""
    
    def __init__(self):
        self.request_count = 0
        self.session_start = datetime.now()
        
    def should_take_break(self):
        """Check if break needed"""
        # Every 25 requests
        if self.request_count > 0 and self.request_count % 25 == 0:
            break_time = random.uniform(60, 120)
            logger.info(f"☕ Break: {break_time:.0f}s after {self.request_count} requests")
            time.sleep(break_time)
            return True
        
        # After 90 minutes
        elapsed = (datetime.now() - self.session_start).seconds
        if elapsed > 5400:
            logger.info(f"🌙 90+ min session, taking 5 min break")
            time.sleep(300)
            self.session_start = datetime.now()
            return True
        
        return False
    
    def increment(self):
        """Track request"""
        self.request_count += 1


# Global instances
rate_limiter = SmartRateLimiter(
    requests_per_5min=8,
    min_delay=4,
    max_delay=10,
    cooldown_after_error=15
)

session_manager = SessionManager()