"""
Pytest configuration and fixtures
"""

import os
import sys
import pytest
from pathlib import Path
from unittest.mock import MagicMock

# Add src to Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / 'src'))
sys.path.insert(0, str(PROJECT_ROOT))

# Fix PySpark environment for local testing:
# - Clear SPARK_HOME to use pip-installed PySpark JARs (avoid version mismatch
#   if a different Spark version is installed system-wide).
# - Set PYSPARK_PYTHON so workers use the same interpreter on Windows (where
#   the default "python3" binary does not exist).
if "SPARK_HOME" in os.environ:
    del os.environ["SPARK_HOME"]

_python_exe = sys.executable
os.environ.setdefault("PYSPARK_PYTHON", _python_exe)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", _python_exe)

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    LongType, FloatType, ArrayType, BooleanType
)


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("tmdb-tests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "2g") \
        .config("spark.ui.enabled", "false") \
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
def raw_movie_schema():
    """Schema matching the raw API data with nested structs"""
    return StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("adult", StringType(), True),
        StructField("budget", LongType(), True),
        StructField("revenue", LongType(), True),
        StructField("release_date", StringType(), True),
        StructField("vote_average", FloatType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("popularity", FloatType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("video", StringType(), True),
        StructField("homepage", StringType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True),
        StructField("belongs_to_collection", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("poster_path", StringType(), True),
            StructField("backdrop_path", StringType(), True),
        ]), True),
        StructField("genres", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])), True),
        StructField("production_companies", ArrayType(StructType([
            StructField("name", StringType(), True),
            StructField("id", IntegerType(), True),
            StructField("logo_path", StringType(), True),
            StructField("origin_country", StringType(), True),
        ])), True),
        StructField("production_countries", ArrayType(StructType([
            StructField("iso_3166_1", StringType(), True),
            StructField("name", StringType(), True),
        ])), True),
        StructField("spoken_languages", ArrayType(StructType([
            StructField("iso_639_1", StringType(), True),
            StructField("name", StringType(), True),
            StructField("english_name", StringType(), True),
        ])), True),
        StructField("cast", ArrayType(StringType()), True),
        StructField("cast_size", IntegerType(), True),
        StructField("director", StringType(), True),
        StructField("crew_size", IntegerType(), True),
    ])


@pytest.fixture
def raw_movie_data(spark_session, raw_movie_schema):
    """Create sample raw movie data that mimics actual API output."""
    from pyspark.sql import Row

    rows = [
        Row(
            id=1, title="Test Movie 1", adult="false",
            budget=100000000, revenue=300000000,
            release_date="2023-01-15", vote_average=7.5, vote_count=1000,
            popularity=50.0, runtime=120, status="Released",
            overview="A great action movie.", tagline="Action never stops",
            original_language="en", video="false",
            homepage="http://example.com", imdb_id="tt0000001",
            original_title="Test Movie 1", poster_path="/poster1.jpg",
            backdrop_path="/backdrop1.jpg",
            belongs_to_collection=Row(id=10, name="Test Collection",
                                      poster_path="/cp1.jpg", backdrop_path="/cb1.jpg"),
            genres=[Row(id=28, name="Action"), Row(id=12, name="Adventure")],
            production_companies=[Row(name="Studio A", id=100, logo_path="/l1.png", origin_country="US")],
            production_countries=[Row(iso_3166_1="US", name="United States")],
            spoken_languages=[Row(iso_639_1="en", name="English", english_name="English")],
            cast=["Actor A", "Actor B", "Actor C"],
            cast_size=3, director="Director X", crew_size=20,
        ),
        Row(
            id=2, title="Test Movie 2", adult="false",
            budget=50000000, revenue=150000000,
            release_date="2023-06-01", vote_average=6.8, vote_count=500,
            popularity=30.0, runtime=90, status="Released",
            overview="A fun comedy.", tagline="Laugh out loud",
            original_language="en", video="false",
            homepage=None, imdb_id="tt0000002",
            original_title="Test Movie 2", poster_path="/poster2.jpg",
            backdrop_path="/backdrop2.jpg",
            belongs_to_collection=None,
            genres=[Row(id=35, name="Comedy")],
            production_companies=[Row(name="Studio B", id=101, logo_path=None, origin_country="UK")],
            production_countries=[Row(iso_3166_1="GB", name="United Kingdom")],
            spoken_languages=[Row(iso_639_1="en", name="English", english_name="English")],
            cast=["Actor D", "Actor E"],
            cast_size=2, director="Director Y", crew_size=15,
        ),
        Row(
            id=3, title="Test Movie 3", adult="false",
            budget=200000000, revenue=800000000,
            release_date="2019-04-26", vote_average=8.4, vote_count=5000,
            popularity=95.0, runtime=181, status="Released",
            overview="An epic sci-fi adventure.", tagline="To infinity",
            original_language="en", video="false",
            homepage="http://example3.com", imdb_id="tt0000003",
            original_title="Test Movie 3", poster_path="/poster3.jpg",
            backdrop_path="/backdrop3.jpg",
            belongs_to_collection=Row(id=10, name="Test Collection",
                                      poster_path="/cp1.jpg", backdrop_path="/cb1.jpg"),
            genres=[Row(id=878, name="Science Fiction"), Row(id=28, name="Action")],
            production_companies=[Row(name="Studio A", id=100, logo_path="/l1.png", origin_country="US")],
            production_countries=[Row(iso_3166_1="US", name="United States")],
            spoken_languages=[Row(iso_639_1="en", name="English", english_name="English")],
            cast=["Actor A", "Actor F"],
            cast_size=2, director="Director X", crew_size=30,
        ),
        Row(
            id=4, title="Test Movie 4", adult="false",
            budget=0, revenue=0,
            release_date="2023-09-15", vote_average=5.0, vote_count=0,
            popularity=10.0, runtime=0, status="Released",
            overview="No Data", tagline="No Data",
            original_language="fr", video="false",
            homepage=None, imdb_id="tt0000004",
            original_title="Test Movie 4 FR", poster_path=None,
            backdrop_path=None,
            belongs_to_collection=None,
            genres=[Row(id=18, name="Drama")],
            production_companies=[Row(name="Studio C", id=102, logo_path=None, origin_country="FR")],
            production_countries=[Row(iso_3166_1="FR", name="France")],
            spoken_languages=[Row(iso_639_1="fr", name="French", english_name="French")],
            cast=None,
            cast_size=0, director=None, crew_size=0,
        ),
        Row(
            id=5, title="Test Movie 5", adult="false",
            budget=75000000, revenue=250000000,
            release_date="2015-07-10", vote_average=7.0, vote_count=800,
            popularity=40.0, runtime=135, status="Released",
            overview="A thrilling ride.", tagline="Hold on tight",
            original_language="en", video="false",
            homepage=None, imdb_id="tt0000005",
            original_title="Test Movie 5", poster_path="/poster5.jpg",
            backdrop_path="/backdrop5.jpg",
            belongs_to_collection=Row(id=20, name="Another Collection",
                                      poster_path="/cp2.jpg", backdrop_path="/cb2.jpg"),
            genres=[Row(id=53, name="Thriller"), Row(id=28, name="Action")],
            production_companies=[Row(name="Studio A", id=100, logo_path="/l1.png", origin_country="US"),
                                  Row(name="Studio B", id=101, logo_path=None, origin_country="UK")],
            production_countries=[Row(iso_3166_1="US", name="United States"),
                                  Row(iso_3166_1="GB", name="United Kingdom")],
            spoken_languages=[Row(iso_639_1="en", name="English", english_name="English"),
                              Row(iso_639_1="fr", name="French", english_name="French")],
            cast=["Actor A", "Actor G", "Actor H"],
            cast_size=3, director="Director Z", crew_size=25,
        ),
    ]

    return spark_session.createDataFrame(rows, raw_movie_schema)


@pytest.fixture
def transformed_movie_data(spark_session):
    """Create sample transformed/processed movie data for analytics tests."""
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("tagline", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("release_year", IntegerType(), True),
        StructField("decade", IntegerType(), True),
        StructField("genres", StringType(), True),
        StructField("belongs_to_collection", StringType(), True),
        StructField("has_franchise", BooleanType(), True),
        StructField("original_language", StringType(), True),
        StructField("budget_musd", DoubleType(), True),
        StructField("budget_category", StringType(), True),
        StructField("revenue_musd", DoubleType(), True),
        StructField("profit_musd", DoubleType(), True),
        StructField("roi", DoubleType(), True),
        StructField("revenue_per_minute", DoubleType(), True),
        StructField("production_companies", StringType(), True),
        StructField("production_countries", StringType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("rating_category", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("overview", StringType(), True),
        StructField("spoken_languages", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True),
        StructField("cast", StringType(), True),
        StructField("cast_size", IntegerType(), True),
        StructField("director", StringType(), True),
        StructField("crew_size", IntegerType(), True),
    ])

    data = [
        (1, "Action Hero", "Boom", "2023-01-15", 2023, 2020, "Action|Adventure",
         "Hero Collection", True, "en", 100.0, "High", 300.0, 200.0, 3.0, 2.5,
         "Studio A", "United States", 1000, 7.5, "Good", 50.0, 120,
         "Great movie", "English", "/p1.jpg", "/b1.jpg",
         "Actor A|Actor B", 2, "Director X", 20),
        (2, "Comedy Fun", "Ha ha", "2023-06-01", 2023, 2020, "Comedy",
         None, False, "en", 50.0, "Medium", 150.0, 100.0, 3.0, 1.67,
         "Studio B", "United Kingdom", 500, 6.8, "Average", 30.0, 90,
         "Funny movie", "English", "/p2.jpg", "/b2.jpg",
         "Actor C|Actor D", 2, "Director Y", 15),
        (3, "Sci-Fi Epic", "Stars", "2019-04-26", 2019, 2010, "Science Fiction|Action",
         "Hero Collection", True, "en", 200.0, "Blockbuster", 800.0, 600.0, 4.0, 4.42,
         "Studio A", "United States", 5000, 8.4, "Very Good", 95.0, 181,
         "Epic adventure", "English", "/p3.jpg", "/b3.jpg",
         "Actor A|Actor E", 2, "Director X", 30),
        (4, "Drama Piece", "Feels", "2023-09-15", 2023, 2020, "Drama",
         None, False, "fr", 10.0, "Low", 25.0, 15.0, 2.5, 0.25,
         "Studio C", "France", 200, 5.0, "Below Average", 10.0, 100,
         "Emotional film", "French", None, None,
         "Actor F", 1, "Director W", 10),
        (5, "Thriller Night", "Suspense", "2015-07-10", 2015, 2010, "Thriller|Action",
         "Night Collection", True, "en", 75.0, "High", 250.0, 175.0, 3.33, 1.85,
         "Studio A|Studio B", "United States|United Kingdom", 800, 7.0, "Good", 40.0, 135,
         "Thrilling", "English|French", "/p5.jpg", "/b5.jpg",
         "Actor A|Actor G|Actor H", 3, "Director Z", 25),
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def test_config():
    """Test configuration"""
    return {
        'api': {
            'base_url': 'https://api.themoviedb.org/3',
            'timeout': 10,
            'retry_attempts': 2,
            'retry_delay': 0,
            'rate_limit': {
                'requests_per_second': 5,
                'burst': 10
            },
            'cache': {
                'enabled': False,
            }
        },
        'processing': {
            'columns_to_drop': [],
            'json_columns': [
                'belongs_to_collection', 'genres',
                'production_countries', 'production_companies', 'spoken_languages'
            ],
            'quality': {
                'min_non_null_columns': 10,
                'min_budget_for_roi': 10.0,
                'min_votes_for_rating': 10,
            },
            'replacements': {
                'zero_to_null': ['budget', 'revenue', 'runtime'],
                'placeholder_values': ['No Data', 'N/A', 'Unknown'],
            },
        },
        'spark': {
            'driver_memory': '2g',
            'executor_memory': '2g',
            'shuffle_partitions': '2'
        },
        'validation': {
            'ranges': {
                'budget_musd': {'min': 0, 'max': 1000},
                'revenue_musd': {'min': 0, 'max': 5000},
                'vote_average': {'min': 0, 'max': 10}
            }
        },
        'kpis': {
            'rankings': {
                'top_n': 3,
                'metrics': [
                    {'name': 'highest_revenue', 'column': 'revenue_musd', 'ascending': False},
                    {'name': 'highest_rated', 'column': 'vote_average', 'ascending': False,
                     'filter': 'vote_count >= 10'},
                ]
            }
        },
        'queries': {
            'search_1': {
                'genres': ['Science Fiction', 'Action'],
                'cast': 'Actor A',
                'sort_by': 'vote_average',
                'ascending': False,
            },
            'search_2': {
                'cast': 'Actor C',
                'director': 'Director Y',
                'sort_by': 'runtime',
                'ascending': True,
            },
        },
        'paths': {
            'raw': './test_data/raw',
            'processed': './test_data/processed',
            'output': './test_data/output'
        },
        'output': {
            'base_path': './test_data/output'
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


@pytest.fixture
def mock_credits_response():
    """Mock TMDB credits API response"""
    return {
        'id': 12345,
        'cast': [
            {'name': 'Actor One', 'character': 'Hero', 'order': 0},
            {'name': 'Actor Two', 'character': 'Villain', 'order': 1},
        ],
        'crew': [
            {'name': 'Director One', 'job': 'Director', 'department': 'Directing'},
            {'name': 'Writer One', 'job': 'Screenplay', 'department': 'Writing'},
        ]
    }


@pytest.fixture(autouse=True)
def setup_test_environment(tmp_path, monkeypatch):
    """Setup test environment"""
    # Create temporary directories
    for subdir in ['raw', 'processed', 'output']:
        (tmp_path / subdir).mkdir(exist_ok=True)

    # Set test environment variables
    monkeypatch.setenv('TMDB_API_KEY', 'test_api_key_12345')
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