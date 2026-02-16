"""
Unit tests for MetricsAggregator
"""

import pytest
from unittest.mock import patch
from pyspark.sql import functions as F

from src.analytics.metrics_aggregator import MetricsAggregator


@pytest.mark.unit
class TestMetricsAggregatorTemporal:
    """Temporal aggregation tests"""

    def test_aggregate_temporal_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_temporal_metrics(transformed_movie_data)

        assert result.count() > 0
        cols = result.columns
        assert "year" in cols
        assert "movie_count" in cols
        assert "total_revenue" in cols
        assert "avg_rating" in cols

    def test_temporal_year_values(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_temporal_metrics(transformed_movie_data)

        years = sorted([r["year"] for r in result.collect()])
        assert 2023 in years
        assert 2019 in years


@pytest.mark.unit
class TestMetricsAggregatorGenre:
    """Genre aggregation tests"""

    def test_aggregate_genre_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_genre_metrics(transformed_movie_data)

        assert result.count() > 0
        genres = [r["genre"] for r in result.collect()]
        assert "Action" in genres

    def test_genre_profitability_rate(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_genre_metrics(transformed_movie_data)

        assert "profitability_rate" in result.columns
        rows = result.collect()
        for row in rows:
            assert 0 <= row["profitability_rate"] <= 100


@pytest.mark.unit
class TestMetricsAggregatorDirector:
    """Director aggregation tests"""

    def test_aggregate_director_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_director_metrics(transformed_movie_data)

        # Only directors with >= 2 movies
        rows = result.collect()
        for row in rows:
            assert row["movie_count"] >= 2
        directors = [r["director"] for r in rows]
        assert "Director X" in directors


@pytest.mark.unit
class TestMetricsAggregatorFranchise:
    """Franchise aggregation tests"""

    def test_aggregate_franchise_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_franchise_metrics(transformed_movie_data)

        assert result.count() > 0
        assert "total_profit" in result.columns
        assert "avg_roi" in result.columns


@pytest.mark.unit
class TestMetricsAggregatorTiers:
    """Budget and rating tier tests"""

    def test_aggregate_budget_tier_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_budget_tier_metrics(transformed_movie_data)

        assert result.count() > 0
        tiers = [r["budget_tier"] for r in result.collect()]
        # We have budget values 10, 50, 75, 100, 200 → should have multiple tiers
        assert len(tiers) >= 2

    def test_aggregate_rating_tier_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        result = agg.aggregate_rating_tier_metrics(transformed_movie_data)

        assert result.count() > 0
        assert "rating_tier" in result.columns


@pytest.mark.unit
class TestMetricsAggregatorCorrelation:
    """Correlation and summary tests"""

    def test_calculate_correlation_metrics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        correlations = agg.calculate_correlation_metrics(transformed_movie_data)

        assert "budget_revenue" in correlations
        assert "budget_rating" in correlations
        # Correlations should be between -1 and 1
        for key, val in correlations.items():
            if val is not None:
                assert -1.0 <= val <= 1.0, f"{key} correlation out of range: {val}"

    def test_generate_summary_statistics(self, transformed_movie_data, test_config):
        agg = MetricsAggregator(test_config)
        summary = agg.generate_summary_statistics(transformed_movie_data)

        assert summary['overview']['total_movies'] == 5
        assert summary['financial']['total_revenue_musd'] > 0
        assert summary['quality']['avg_rating'] > 0

    def test_export_all_metrics(self, transformed_movie_data, test_config, tmp_path):
        agg = MetricsAggregator(test_config)
        output_path = str(tmp_path / "metrics_export")

        # Mock parquet writes to avoid Hadoop native library issues on Windows
        with patch("pyspark.sql.readwriter.DataFrameWriter.parquet"):
            paths = agg.export_all_metrics(transformed_movie_data, output_path)

        assert "temporal" in paths
        assert "genre" in paths
        assert "franchise" in paths
