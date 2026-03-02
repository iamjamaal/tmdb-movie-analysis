"""
TMDB API Client with Rate Limiting, Caching, and Error Handling.

Retry strategy — two independent layers:
  1. Transport (urllib3): retries HTTP 429/5xx at the adapter level before the
     application sees the response.
  2. Application (tenacity): retries on Timeout/ConnectionError with exponential
     backoff, giving full visibility in logs via before_sleep_log.
"""

import os
import time
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import redis
import json
from threading import Lock
import hashlib
from tenacity import (
    Retrying,
    RetryError,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)


logger = logging.getLogger(__name__)


@dataclass
class RateLimiter:
    """Token bucket rate limiter"""
    requests_per_second: float
    burst: int
    
    def __post_init__(self):
        self.tokens = self.burst
        self.last_update = time.time()
        self.lock = Lock()
    
    def acquire(self):
        """Acquire a token, blocking if necessary"""
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            
            # Add tokens based on elapsed time
            self.tokens = min(
                self.burst,
                self.tokens + elapsed * self.requests_per_second
            )
            self.last_update = now
            
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.requests_per_second
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class CacheManager:
    """Redis-based cache manager with fallback to in-memory cache"""
    
    def __init__(self, redis_host: str = 'redis', redis_port: int = 6379, ttl: int = 3600):
        self.ttl = ttl
        self.memory_cache: Dict[str, Any] = {}
        
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                decode_responses=True,
                socket_connect_timeout=2
            )
            self.redis_client.ping()
            self.use_redis = True
            logger.info("Redis cache connected successfully")
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logger.warning(f"Redis connection failed, using in-memory cache: {e}")
            self.redis_client = None
            self.use_redis = False
    
    def _get_cache_key(self, key: str) -> str:
        """Generate cache key with namespace"""
        return f"tmdb:api:{hashlib.md5(key.encode()).hexdigest()}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        cache_key = self._get_cache_key(key)
        
        if self.use_redis and self.redis_client:
            try:
                value = self.redis_client.get(cache_key)
                if value:
                    logger.debug(f"Cache hit (Redis): {key}")
                    return json.loads(value)
            except Exception as e:
                logger.error(f"Redis get error: {e}")
        
        # Fallback to memory cache
        if cache_key in self.memory_cache:
            logger.debug(f"Cache hit (Memory): {key}")
            return self.memory_cache[cache_key]
        
        logger.debug(f"Cache miss: {key}")
        return None
    
    def set(self, key: str, value: Any):
        """Set value in cache"""
        cache_key = self._get_cache_key(key)
        json_value = json.dumps(value)
        
        if self.use_redis and self.redis_client:
            try:
                self.redis_client.setex(cache_key, self.ttl, json_value)
                logger.debug(f"Cached in Redis: {key}")
                return
            except Exception as e:
                logger.error(f"Redis set error: {e}")
        
        # Fallback to memory cache
        self.memory_cache[cache_key] = value
        logger.debug(f"Cached in memory: {key}")


class TMDBClient:
    """
    Robust TMDB API Client with:
    - Rate limiting
    - Retry logic
    - Caching
    - Comprehensive error handling
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize TMDB API client
        
        Args:
            config: Configuration dictionary
        """
        self.api_key = os.getenv('TMDB_API_KEY')
        if not self.api_key:
            raise ValueError("TMDB_API_KEY environment variable not set")
        
        self.base_url = config.get('api', {}).get('base_url', 'https://api.themoviedb.org/3')
        self.timeout = config.get('api', {}).get('timeout', 30)

        # Application-level retry settings (separate from urllib3 transport-level retry)
        self.app_retry_attempts = config.get('api', {}).get('retry_attempts', 3)
        self.app_retry_delay = config.get('api', {}).get('retry_delay', 2)

        # Rate limiting
        rate_limit_config = config.get('api', {}).get('rate_limit', {})
        self.rate_limiter = RateLimiter(
            requests_per_second=rate_limit_config.get('requests_per_second', 40),
            burst=rate_limit_config.get('burst', 50)
        )
        
        # Caching
        cache_config = config.get('api', {}).get('cache', {})
        self.cache_enabled = cache_config.get('enabled', True)
        if self.cache_enabled:
            self.cache = CacheManager(
                redis_host=cache_config.get('redis_host', 'redis'),
                redis_port=cache_config.get('redis_port', 6379),
                ttl=cache_config.get('ttl', 3600)
            )
        
        # HTTP session with retry logic
        self.session = self._create_session(config)
        
        logger.info("TMDBClient initialized successfully")
    
    def _create_session(self, config: Dict[str, Any]) -> requests.Session:
        """Create requests session with retry strategy"""
        session = requests.Session()
        
        retry_config = config.get('api', {})
        retry_strategy = Retry(
            total=retry_config.get('retry_attempts', 3),
            backoff_factor=retry_config.get('retry_delay', 2),
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make API request with rate limiting, caching, and tenacity-powered retry.

        Retry behaviour (tenacity):
          - Retries on Timeout and ConnectionError (transient network issues).
          - Exponential backoff: wait = retry_delay * 2^n, capped at retry_delay * 10.
          - Each sleep is logged via before_sleep_log so retries are fully visible.
          - HTTP 404 is treated as permanent and short-circuits immediately.
          - All other HTTP errors (after urllib3 transport retries are exhausted)
            are logged and return None without further retrying.

        Args:
            endpoint: API endpoint path (e.g. "movie/299534")
            params: Optional query parameters dict

        Returns:
            Parsed JSON dict on success, or None if all retries are exhausted
        """
        # Serve from cache before touching the network
        cache_key = f"{endpoint}:{json.dumps(params or {}, sort_keys=True)}"
        if self.cache_enabled:
            cached_response = self.cache.get(cache_key)
            if cached_response:
                return cached_response

        url = f"{self.base_url}/{endpoint}"
        request_params = dict(params or {})
        request_params['api_key'] = self.api_key

        try:
            # tenacity retries on transient network errors with exponential backoff.
            # urllib3's Retry (mounted on the session) handles 429/5xx at the transport
            # level before we ever reach this loop.
            for attempt in Retrying(
                retry=retry_if_exception_type((
                    requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                )),
                stop=stop_after_attempt(self.app_retry_attempts),
                wait=wait_exponential(
                    multiplier=self.app_retry_delay,
                    min=self.app_retry_delay,
                    max=self.app_retry_delay * 10,
                ),
                before_sleep=before_sleep_log(logger, logging.INFO),
                reraise=False,
            ):
                with attempt:
                    self.rate_limiter.acquire()
                    response = self.session.get(url, params=request_params, timeout=self.timeout)
                    response.raise_for_status()
                    data = response.json()
                    if self.cache_enabled:
                        self.cache.set(cache_key, data)
                    logger.debug(f"API request successful: {endpoint}")
                    return data

        except RetryError:
            logger.error(f"All {self.app_retry_attempts} tenacity retry attempts exhausted for {endpoint}")
            return None

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Resource not found: {endpoint}")
            else:
                logger.error(f"HTTP error for {endpoint}: {e}")
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error for {endpoint}: {e}")
            return None
    
    def get_movie(self, movie_id: int) -> Optional[Dict]:
        """
        Fetch movie details
        
        Args:
            movie_id: TMDB movie ID
            
        Returns:
            Movie data dictionary or None
        """
        logger.info(f"Fetching movie ID: {movie_id}")
        return self._make_request(f"movie/{movie_id}")
    
    def get_movie_credits(self, movie_id: int) -> Optional[Dict]:
        """
        Fetch movie credits (cast and crew)
        
        Args:
            movie_id: TMDB movie ID
            
        Returns:
            Credits data dictionary or None
        """
        logger.debug(f"Fetching credits for movie ID: {movie_id}")
        return self._make_request(f"movie/{movie_id}/credits")
    
    def get_complete_movie_data(self, movie_id: int) -> Optional[Dict]:
        """
        Fetch complete movie data including credits
        
        Args:
            movie_id: TMDB movie ID
            
        Returns:
            Combined movie and credits data or None
        """
        movie_data = self.get_movie(movie_id)
        if not movie_data:
            logger.warning(f"No data found for movie ID: {movie_id}")
            return None
        
        credits_data = self.get_movie_credits(movie_id)
        
        if credits_data:
            # Extract cast and crew information
            cast = credits_data.get('cast', [])
            crew = credits_data.get('crew', [])
            
            # Add cast information
            movie_data['cast'] = [member['name'] for member in cast[:10]]  # Top 10 cast
            movie_data['cast_size'] = len(cast)
            
            # Find director
            director = next((member['name'] for member in crew if member['job'] == 'Director'), None)
            movie_data['director'] = director
            movie_data['crew_size'] = len(crew)
        else:
            movie_data['cast'] = []
            movie_data['cast_size'] = 0
            movie_data['director'] = None
            movie_data['crew_size'] = 0
        
        return movie_data
    
    def fetch_movies_batch(self, movie_ids: List[int]) -> List[Dict]:
        """
        Fetch multiple movies with progress tracking (parallel with threading)
        
        Args:
            movie_ids: List of TMDB movie IDs
            
        Returns:
            List of movie data dictionaries
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        movies = []
        total = len(movie_ids)
        
        logger.info(f"Fetching {total} movies in parallel...")
        
        # Use thread pool for parallel requests (rate limiter handles coordination)
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Submit all tasks
            future_to_id = {
                executor.submit(self.get_complete_movie_data, movie_id): movie_id 
                for movie_id in movie_ids
            }
            
            # Process completed tasks
            completed = 0
            for future in as_completed(future_to_id):
                movie_id = future_to_id[future]
                try:
                    movie_data = future.result()
                    if movie_data:
                        movies.append(movie_data)
                    completed += 1
                    
                    if completed % 5 == 0 or completed == total:
                        logger.info(f"Progress: {completed}/{total} movies fetched ({(completed/total)*100:.0f}%)")
                        
                except Exception as e:
                    logger.error(f"Error fetching movie {movie_id}: {e}")
                    completed += 1
        
        logger.info(f"Successfully fetched {len(movies)}/{total} movies")
        return movies
    
    def close(self):
        """Close session"""
        self.session.close()
        logger.info("TMDBClient session closed")