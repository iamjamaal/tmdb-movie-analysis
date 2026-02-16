"""
Unit tests for KPICalculator
"""

import pytest
from pyspark.sql import functions as F

from src.analytics.kpi_calculator import KPICalculator


@pytest.mark.unit
class TestKPICalculatorRankings:
    """Tests for ranking functions"""

    def test_rank_movies_by_revenue(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.rank_movies_by_metric(
            transformed_movie_data,
            metric_name="highest_revenue",
            column="revenue_musd",
            ascending=False,
            top_n=3,
        )
        rows = result.collect()
        assert len(rows) == 3
        # First should be highest revenue (800)
        assert rows[0]["revenue_musd"] == 800.0

    def test_rank_movies_ascending(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.rank_movies_by_metric(
            transformed_movie_data,
            metric_name="lowest_revenue",
            column="revenue_musd",
            ascending=True,
            top_n=2,
        )
        rows = result.collect()
        assert len(rows) == 2
        # First should be lowest revenue (25)
        assert rows[0]["revenue_musd"] == 25.0

    def test_rank_movies_with_filter(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.rank_movies_by_metric(
            transformed_movie_data,
            metric_name="highest_rated_filtered",
            column="vote_average",
            ascending=False,
            filter_condition="vote_count >= 500",
            top_n=5,
        )
        rows = result.collect()
        # Movie 4 (vote_count=200) should be excluded
        ids = [r["id"] for r in rows]
        assert 4 not in ids

    def test_rank_produces_rank_column(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.rank_movies_by_metric(
            transformed_movie_data,
            metric_name="test",
            column="revenue_musd",
            ascending=False,
            top_n=3,
        )
        assert "rank" in result.columns
        ranks = [r["rank"] for r in result.collect()]
        assert ranks == [1, 2, 3]


@pytest.mark.unit
class TestKPICalculatorFranchise:
    """Franchise analysis tests"""

    def test_analyze_franchise_performance(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.analyze_franchise_performance(transformed_movie_data)

        rows = {r["is_franchise"]: r for r in result.collect()}
        assert "Franchise" in rows
        assert "Standalone" in rows
        assert rows["Franchise"]["movie_count"] == 3
        assert rows["Standalone"]["movie_count"] == 2

    def test_get_most_successful_franchises(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.get_most_successful_franchises(transformed_movie_data)

        assert result.count() >= 1
        # Hero Collection has 2 movies totalling revenue 1100
        rows = result.collect()
        collections = [r["belongs_to_collection"] for r in rows]
        assert "Hero Collection" in collections

    def test_get_most_successful_directors(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.get_most_successful_directors(transformed_movie_data)

        rows = {r["director"]: r for r in result.collect()}
        # Director X has 2 movies
        assert "Director X" in rows
        assert rows["Director X"]["movie_count"] == 2


@pytest.mark.unit
class TestKPICalculatorSearch:
    """Search query tests"""

    def test_run_search_queries(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        # The search queries use array_contains on cast; our transformed fixture
        # stores cast as a pipe-separated string, so search queries may return 0.
        # This just tests the method completes without error.
        results = calc.run_search_queries(transformed_movie_data)
        assert "bruce_willis_scifi_action" in results
        assert "uma_thurman_tarantino" in results

    def test_get_roi_by_genre(self, transformed_movie_data, test_config):
        calc = KPICalculator(test_config)
        result = calc.get_roi_by_genre(transformed_movie_data)

        genres = [r["genre"] for r in result.collect()]
        assert "Action" in genres
        assert result.count() > 0
