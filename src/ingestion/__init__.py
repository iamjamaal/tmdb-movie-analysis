"""
Data Ingestion Module for TMDB Movie Analysis
"""

from .data_fetcher import DataFetcher
from .api_client import APIClient

__all__ = ['DataFetcher', 'APIClient']