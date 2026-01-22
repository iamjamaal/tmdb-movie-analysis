"""
Data ingestion module for TMDB API
"""

from .data_fetcher import DataFetcher
from .api_client import TMDBClient

__all__ = ['DataFetcher', 'TMDBClient']
