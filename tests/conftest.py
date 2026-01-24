"""
Pytest configuration and fixtures
"""

import os
import sys
import pytest
from pathlib import Path

# Add src to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("tmdb-tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_movie_schema():
    """Sample movie data schema"""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("budget", DoubleType(), True),
        StructField("revenue", DoubleType(), True),
        StructField("release_date", StringType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("genres", StringType(), True),
        StructField("original_language", StringType(), True)
    ])


@pytest.fixture
def sample_movie_data(spark_session, sample_movie_schema):
    """Create sample movie data for testing"""
    data = [
        (1, "Test Movie 1", 100.0, 300.0, "2023-01-01", 7.5, 1000, 50.0, "Action|Adventure", "en"),
        (2, "Test Movie 2", 50.0, 150.0, "2023-02-01", 6.8, 500, 30.0, "Drama", "en"),
        (3, "Test Movie 3", 200.0, 500.0, "2023-03-01", 8.2, 2000, 80.0, "Sci-Fi|Action", "en"),
        (4, "Test Movie 4", 75.0, 100.0, "2023-04-01", 5.5, 300, 20.0, "Comedy", "en"),
        (5, "Test Movie 5", 150.0, 450.0, "2023-05-01", 7.8, 1500, 60.0, "Thriller", "en")
    ]
    
    return spark_session.createDataFrame(data, sample_movie_schema)


@pytest.fixture
def test_config():
    """Test configuration"""
    return {
        'api': {
            'base_url': 'https://api.themoviedb.org/3',
            'timeout': 10,
            'rate_limit': {
                'requests_per_second': 5,
                'burst': 10
            }
        },
        'spark': {
            'driver_memory': '2g',
            'executor_memory': '2g',
            'shuffle_partitions': '2'
        },
        'validation': {
            'ranges': {
                'budget': {'min': 0, 'max': 1000},
                'revenue': {'min': 0, 'max': 5000},
                'vote_average': {'min': 0, 'max': 10}
            }
        },
        'paths': {
            'raw': './test_data/raw',
            'processed': './test_data/processed',
            'output': './test_data/output'
        }
    }


@pytest.fixture
def mock_tmdb_response():
    """Mock TMDB API response"""
    return {
        'id': 12345,
        'title': 'Mock Movie',
        'budget': 100000000,
        'revenue': 300000000,
        'release_date': '2023-01-01',
        'vote_average': 7.5,
        'vote_count': 1000,
        'popularity': 50.0,
        'genres': [
            {'id': 28, 'name': 'Action'},
            {'id': 12, 'name': 'Adventure'}
        ],
        'original_language': 'en',
        'overview': 'This is a test movie overview.',
        'tagline': 'Test tagline',
        'runtime': 120,
        'status': 'Released',
        'production_companies': [
            {'id': 1, 'name': 'Test Studio'}
        ],
        'production_countries': [
            {'iso_3166_1': 'US', 'name': 'United States'}
        ],
        'spoken_languages': [
            {'iso_639_1': 'en', 'name': 'English'}
        ]
    }


@pytest.fixture(autouse=True)
def setup_test_environment(tmp_path, monkeypatch):
    """Setup test environment"""
    # Create temporary directories
    for subdir in ['raw', 'processed', 'output']:
        (tmp_path / subdir).mkdir(exist_ok=True)
    
    # Set test environment variables
    monkeypatch.setenv('TMDB_API_KEY', 'test_api_key')
    monkeypatch.setenv('ENVIRONMENT', 'testing')
    monkeypatch.setenv('LOG_LEVEL', 'ERROR')
    
    yield
    
    # Cleanup is handled by tmp_path fixture


@pytest.fixture
def cleanup_spark_session(spark_session):
    """Clean up Spark session after tests"""
    yield
    spark_session.catalog.clearCache()


# Pytest configuration
def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )