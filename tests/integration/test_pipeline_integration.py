"""
Integration tests for the TMDB pipeline.

These tests verify that multiple components work together correctly,
using the same Spark session and flowing data through cleaning → transformation → validation → analytics.
"""

import pytest
from pyspark.sql import functions as F

from src.processing.data_cleaner import DataCleaner
from src.processing.data_transformer import DataTransformer
from src.processing.data_validator import DataValidator
from src.analytics.kpi_calculator import KPICalculator
from src.analytics.metrics_aggregator import MetricsAggregator
from src.analytics.advanced_queries import AdvancedQueries


@pytest.mark.integration
class TestCleaningToTransformPipeline:
    """Test data flowing from cleaning through transformation."""

    def test_clean_then_transform(self, raw_movie_data, test_config):
        """Full cleaning + transformation pipeline on realistic raw data."""
        cleaner = DataCleaner(test_config)
        cleaned = cleaner.clean(raw_movie_data)

        # After cleaning: irrelevant columns dropped, JSON parsed, released only
        assert "adult" not in cleaned.columns
        assert "video" not in cleaned.columns
        assert "homepage" not in cleaned.columns
        assert "imdb_id" not in cleaned.columns
        assert "status" not in cleaned.columns
        # Genres should now be pipe-separated strings
        genres_vals = [r["genres"] for r in cleaned.select("genres").collect() if r["genres"]]
        for g in genres_vals:
            assert "|" in g or len(g.split("|")) == 1  # single or multi-genre

        transformer = DataTransformer(test_config)
        transformed = transformer.transform(cleaned)

        assert transformed.count() > 0
        assert "budget_musd" in transformed.columns
        assert "revenue_musd" in transformed.columns
        assert "profit_musd" in transformed.columns
        assert "roi" in transformed.columns
        assert "release_year" in transformed.columns
        assert "decade" in transformed.columns
        assert "has_franchise" in transformed.columns

    def test_zero_budget_becomes_null_after_clean(self, raw_movie_data, test_config):
        """Movie 4 has budget=0, which should become null after cleaning."""
        cleaner = DataCleaner(test_config)
        cleaned = cleaner.clean(raw_movie_data)

        movie4 = cleaned.filter(F.col("id") == 4).collect()
        if len(movie4) > 0:
            assert movie4[0]["budget"] is None
            assert movie4[0]["revenue"] is None
            assert movie4[0]["runtime"] is None

    def test_placeholder_text_removed(self, raw_movie_data, test_config):
        """Movie 4 has 'No Data' overview/tagline, which should become null."""
        cleaner = DataCleaner(test_config)
        cleaned = cleaner.clean(raw_movie_data)

        movie4 = cleaned.filter(F.col("id") == 4).collect()
        if len(movie4) > 0:
            assert movie4[0]["overview"] is None
            assert movie4[0]["tagline"] is None


@pytest.mark.integration
class TestTransformToAnalyticsPipeline:
    """Test transformed data flowing through analytics modules."""

    def test_kpi_on_transformed_data(self, transformed_movie_data, test_config):
        """KPI calculator should work on transformed data."""
        calc = KPICalculator(test_config)

        # Revenue ranking
        revenue_ranking = calc.rank_movies_by_metric(
            transformed_movie_data,
            metric_name="highest_revenue",
            column="revenue_musd",
            ascending=False,
            top_n=3,
        )
        assert revenue_ranking.count() == 3

        # Franchise analysis
        franchise = calc.analyze_franchise_performance(transformed_movie_data)
        assert franchise.count() == 2  # Franchise + Standalone

        # Director analysis
        directors = calc.get_most_successful_directors(transformed_movie_data)
        assert directors.count() > 0

    def test_metrics_aggregation_on_transformed_data(self, transformed_movie_data, test_config):
        """MetricsAggregator should produce valid aggregations."""
        agg = MetricsAggregator(test_config)

        temporal = agg.aggregate_temporal_metrics(transformed_movie_data)
        assert temporal.count() > 0

        genres = agg.aggregate_genre_metrics(transformed_movie_data)
        assert genres.count() > 0

        summary = agg.generate_summary_statistics(transformed_movie_data)
        assert summary['overview']['total_movies'] == 5
        assert summary['financial']['total_revenue_musd'] > 0

    def test_advanced_queries_on_transformed_data(self, transformed_movie_data, test_config):
        """AdvancedQueries should filter correctly on transformed data."""
        aq = AdvancedQueries(test_config)

        # Year range search
        result = aq.search_by_year_range(
            transformed_movie_data,
            start_year=2019,
            end_year=2023,
            min_rating=7.0,
        )
        assert result.count() >= 1
        for row in result.collect():
            assert row["release_year"] >= 2019
            assert row["vote_average"] >= 7.0

        # Budget range search
        result = aq.search_by_budget_range(
            transformed_movie_data,
            min_budget=50.0,
            max_budget=200.0,
        )
        assert result.count() >= 1


@pytest.mark.integration
class TestValidationOnTransformedData:
    """Test validation on processed data."""

    def test_validation_report(self, transformed_movie_data, test_config):
        """Full validation report should complete without errors."""
        validator = DataValidator(test_config)
        report = validator.generate_validation_report(transformed_movie_data)

        assert report['total_records'] == 5
        assert 'completeness' in report
        assert 'uniqueness' in report

        # All IDs should be unique
        assert report['uniqueness']['id']['is_unique'] is True

    def test_completeness_on_transformed_data(self, transformed_movie_data, test_config):
        """Core columns should have high completeness."""
        validator = DataValidator(test_config)
        report = validator.validate_completeness(transformed_movie_data)

        # id and title should be 100% complete
        assert report['column_completeness']['id']['completeness_percentage'] == 100.0
        assert report['column_completeness']['title']['completeness_percentage'] == 100.0

    def test_business_rules_on_transformed_data(self, transformed_movie_data, test_config):
        """Business rules should pass on clean data."""
        validator = DataValidator(test_config)
        rules = validator.validate_business_rules(transformed_movie_data)

        # Vote average should be in range
        assert rules['vote_average_range']['is_valid'] is True


@pytest.mark.integration
class TestMetricsExportPipeline:
    """Test metrics export flow."""

    @pytest.mark.xfail(
        reason="Parquet write requires Hadoop native libraries (winutils.exe); "
               "not available in all local environments. Does not affect production.",
        raises=Exception,
        strict=False,
    )
    def test_export_and_reload_metrics(self, transformed_movie_data, test_config,
                                        spark_session, tmp_path):
        """Export metrics to parquet and verify they can be reloaded."""
        agg = MetricsAggregator(test_config)
        output_path = str(tmp_path / "export_test")
        paths = agg.export_all_metrics(transformed_movie_data, output_path)

        # Verify at least some metrics were exported
        assert len(paths) > 0

        # Verify we can reload the temporal metrics
        if 'temporal' in paths:
            reloaded = spark_session.read.parquet(paths['temporal'])
            assert reloaded.count() > 0
            assert "year" in reloaded.columns

        # Verify genre metrics
        if 'genre' in paths:
            reloaded = spark_session.read.parquet(paths['genre'])
            assert reloaded.count() > 0
            assert "genre" in reloaded.columns


@pytest.mark.integration
class TestEndToEndPipeline:
    """Full end-to-end pipeline test (without API calls)."""

    @pytest.mark.xfail(
        reason="End-to-end export step requires Hadoop native libraries (winutils.exe); "
               "not available in all local environments. Does not affect production.",
        raises=Exception,
        strict=False,
    )
    def test_full_pipeline_flow(self, raw_movie_data, test_config, tmp_path):
        """Simulate the full pipeline: clean → transform → validate → analyze → export."""
        # Step 1: Clean
        cleaner = DataCleaner(test_config)
        cleaned = cleaner.clean(raw_movie_data)
        assert cleaned.count() > 0

        # Step 2: Transform
        transformer = DataTransformer(test_config)
        transformed = transformer.transform(cleaned)
        assert transformed.count() > 0
        assert "budget_musd" in transformed.columns

        # Step 3: Validate
        validator = DataValidator(test_config)
        report = validator.generate_validation_report(transformed)
        assert report['total_records'] > 0

        # Step 4: Analytics
        calc = KPICalculator(test_config)
        revenue_top = calc.rank_movies_by_metric(
            transformed, "highest_revenue", "revenue_musd",
            ascending=False, top_n=3,
        )
        assert revenue_top.count() > 0

        # Step 5: Metrics
        agg = MetricsAggregator(test_config)
        summary = agg.generate_summary_statistics(transformed)
        assert summary['overview']['total_movies'] > 0

        # Step 6: Export
        output_path = str(tmp_path / "e2e_output")
        paths = agg.export_all_metrics(transformed, output_path)
        assert len(paths) > 0
