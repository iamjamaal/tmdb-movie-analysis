"""
Unit tests for TMDBClient, RateLimiter, and CacheManager
"""

import time
import json
import pytest
from unittest.mock import patch, MagicMock, PropertyMock

from src.ingestion.api_client import RateLimiter, CacheManager, TMDBClient


# ---------------------------------------------------------------------------
# RateLimiter
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestRateLimiter:
    """Tests for the token-bucket RateLimiter"""

    def test_initial_tokens_equal_burst(self):
        rl = RateLimiter(requests_per_second=10, burst=5)
        assert rl.tokens == 5

    def test_acquire_decrements_token(self):
        rl = RateLimiter(requests_per_second=10, burst=5)
        rl.acquire()
        # After one acquire the tokens should have dropped by ~1
        assert rl.tokens < 5

    def test_acquire_multiple_times(self):
        rl = RateLimiter(requests_per_second=100, burst=10)
        for _ in range(5):
            rl.acquire()
        # Should still have tokens remaining
        assert rl.tokens >= 0

    def test_acquire_blocks_when_empty(self):
        rl = RateLimiter(requests_per_second=10, burst=1)
        rl.tokens = 0
        rl.last_update = time.time()
        start = time.time()
        rl.acquire()
        elapsed = time.time() - start
        # Should have slept ~0.1s (1 / 10 rps)
        assert elapsed >= 0.05


# ---------------------------------------------------------------------------
# CacheManager
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCacheManager:
    """Tests for in-memory fallback cache (Redis not available in CI)"""

    def test_memory_cache_set_and_get(self):
        cm = CacheManager(redis_host="nonexistent", redis_port=9999, ttl=60)
        assert cm.use_redis is False  # Should fall back

        cm.set("movie_123", {"title": "Test"})
        result = cm.get("movie_123")
        assert result == {"title": "Test"}

    def test_cache_miss_returns_none(self):
        cm = CacheManager(redis_host="nonexistent", redis_port=9999, ttl=60)
        assert cm.get("nonexistent_key") is None

    def test_cache_key_hashing(self):
        cm = CacheManager(redis_host="nonexistent", redis_port=9999, ttl=60)
        key = cm._get_cache_key("movie/12345")
        assert key.startswith("tmdb:api:")
        assert len(key) > len("tmdb:api:")

    def test_cache_overwrites_value(self):
        cm = CacheManager(redis_host="nonexistent", redis_port=9999, ttl=60)
        cm.set("key1", {"v": 1})
        cm.set("key1", {"v": 2})
        assert cm.get("key1") == {"v": 2}


# ---------------------------------------------------------------------------
# TMDBClient
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestTMDBClient:
    """Tests for TMDBClient (API calls are mocked)"""

    def _make_client(self, test_config):
        """Helper: create client with cache disabled and no real Redis."""
        config = dict(test_config)
        config['api'] = dict(config['api'])
        config['api']['cache'] = {'enabled': False}
        return TMDBClient(config)

    def test_init_raises_without_api_key(self, monkeypatch, test_config):
        monkeypatch.delenv('TMDB_API_KEY', raising=False)
        with pytest.raises(ValueError, match="TMDB_API_KEY"):
            TMDBClient(test_config)

    def test_init_success(self, test_config):
        client = self._make_client(test_config)
        assert client.base_url == "https://api.themoviedb.org/3"
        assert client.timeout == 10
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_get_movie_success(self, mock_get, test_config, mock_tmdb_response):
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_tmdb_response
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        client = self._make_client(test_config)
        result = client.get_movie(12345)

        assert result is not None
        assert result['id'] == 12345
        assert result['title'] == 'Mock Movie'
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_get_movie_404_returns_none(self, mock_get, test_config):
        import requests as req
        mock_resp = MagicMock()
        mock_resp.status_code = 404
        http_err = req.exceptions.HTTPError(response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_err
        mock_get.return_value = mock_resp

        client = self._make_client(test_config)
        result = client.get_movie(99999999)
        assert result is None
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_get_movie_timeout_returns_none(self, mock_get, test_config):
        import requests as req
        mock_get.side_effect = req.exceptions.Timeout("timed out")

        client = self._make_client(test_config)
        result = client.get_movie(12345)
        assert result is None
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_get_movie_credits(self, mock_get, test_config, mock_credits_response):
        mock_resp = MagicMock()
        mock_resp.json.return_value = mock_credits_response
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        client = self._make_client(test_config)
        result = client.get_movie_credits(12345)
        assert result is not None
        assert 'cast' in result
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_get_complete_movie_data(self, mock_get, test_config,
                                      mock_tmdb_response, mock_credits_response):
        """get_complete_movie_data merges movie + credits."""
        mock_resp_movie = MagicMock()
        mock_resp_movie.json.return_value = mock_tmdb_response
        mock_resp_movie.raise_for_status = MagicMock()

        mock_resp_credits = MagicMock()
        mock_resp_credits.json.return_value = mock_credits_response
        mock_resp_credits.raise_for_status = MagicMock()

        mock_get.side_effect = [mock_resp_movie, mock_resp_credits]

        client = self._make_client(test_config)
        result = client.get_complete_movie_data(12345)

        assert result['title'] == 'Mock Movie'
        assert result['director'] == 'Director One'
        assert 'Actor One' in result['cast']
        assert result['cast_size'] == 2
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_get_complete_movie_data_no_credits(self, mock_get, test_config,
                                                  mock_tmdb_response):
        """When credits call fails, defaults are filled in."""
        mock_resp_movie = MagicMock()
        mock_resp_movie.json.return_value = mock_tmdb_response
        mock_resp_movie.raise_for_status = MagicMock()

        import requests as req
        mock_resp_credits = MagicMock()
        mock_resp_credits.raise_for_status.side_effect = req.exceptions.Timeout()
        mock_get.side_effect = [mock_resp_movie,
                                req.exceptions.Timeout("timeout")]

        client = self._make_client(test_config)
        result = client.get_complete_movie_data(12345)

        assert result['director'] is None
        assert result['cast'] == []
        assert result['cast_size'] == 0
        client.close()

    @patch("src.ingestion.api_client.requests.Session.get")
    def test_fetch_movies_batch(self, mock_get, test_config, mock_tmdb_response,
                                 mock_credits_response):
        """fetch_movies_batch returns list of movie dicts."""
        mock_resp_movie = MagicMock()
        mock_resp_movie.json.return_value = mock_tmdb_response
        mock_resp_movie.raise_for_status = MagicMock()

        mock_resp_credits = MagicMock()
        mock_resp_credits.json.return_value = mock_credits_response
        mock_resp_credits.raise_for_status = MagicMock()

        mock_get.return_value = mock_resp_movie  # simplified: same response each time
        # A rough mock — get_complete_movie_data calls get twice per movie
        mock_get.side_effect = None
        mock_get.return_value = mock_resp_movie

        client = self._make_client(test_config)
        with patch.object(client, 'get_complete_movie_data',
                          return_value=mock_tmdb_response):
            results = client.fetch_movies_batch([1, 2, 3])

        assert len(results) == 3
        client.close()

    def test_close_session(self, test_config):
        client = self._make_client(test_config)
        client.close()  # Should not raise
