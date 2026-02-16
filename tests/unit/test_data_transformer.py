"""
Unit tests for DataTransformer
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from src.processing.data_transformer import DataTransformer


@pytest.mark.unit
class TestDataTransformerFinancial:
    """Financial transformation tests"""

    def test_convert_to_millions(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("budget", DoubleType(), True),
            StructField("revenue", DoubleType(), True),
        ])
        data = [(1, 150_000_000.0, 400_000_000.0)]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.convert_to_millions(df)

        row = result.collect()[0]
        assert abs(row["budget_musd"] - 150.0) < 0.01
        assert abs(row["revenue_musd"] - 400.0) < 0.01
        assert "budget" not in result.columns  # Original dropped
        assert "revenue" not in result.columns

    def test_calculate_profit(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("budget_musd", DoubleType(), True),
            StructField("revenue_musd", DoubleType(), True),
        ])
        data = [
            (1, 100.0, 300.0),
            (2, None, 200.0),   # budget null → profit null
            (3, 50.0, None),    # revenue null → profit null
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.calculate_profit(df)

        rows = {r["id"]: r for r in result.collect()}
        assert abs(rows[1]["profit_musd"] - 200.0) < 0.01
        assert rows[2]["profit_musd"] is None
        assert rows[3]["profit_musd"] is None

    def test_calculate_roi(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("budget_musd", DoubleType(), True),
            StructField("revenue_musd", DoubleType(), True),
        ])
        data = [
            (1, 100.0, 300.0),  # ROI = 3.0
            (2, 0.0, 200.0),    # budget 0 → null
            (3, None, 200.0),   # budget null → null
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.calculate_roi(df)

        rows = {r["id"]: r for r in result.collect()}
        assert abs(rows[1]["roi"] - 3.0) < 0.01
        assert rows[2]["roi"] is None
        assert rows[3]["roi"] is None

    def test_calculate_revenue_per_minute(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("revenue_musd", DoubleType(), True),
            StructField("runtime", IntegerType(), True),
        ])
        data = [
            (1, 300.0, 120),  # 2.5 per min
            (2, 100.0, 0),    # runtime 0 → null
            (3, None, 90),    # revenue null → null
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.calculate_revenue_per_minute(df)

        rows = {r["id"]: r for r in result.collect()}
        assert abs(rows[1]["revenue_per_minute"] - 2.5) < 0.01
        assert rows[2]["revenue_per_minute"] is None
        assert rows[3]["revenue_per_minute"] is None


@pytest.mark.unit
class TestDataTransformerCategorical:
    """Categorical transformation tests"""

    def test_extract_release_year(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("release_date", StringType(), True),
        ])
        data = [
            (1, "2023-06-15"),
            (2, "1999-12-31"),
            (3, None),
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.extract_release_year(df)

        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["release_year"] == 2023
        assert rows[2]["release_year"] == 1999
        assert rows[3]["release_year"] is None

    def test_add_franchise_flag(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("belongs_to_collection", StringType(), True),
        ])
        data = [
            (1, "Avengers Collection"),
            (2, None),
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.add_franchise_flag(df)

        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["has_franchise"] is True
        assert rows[2]["has_franchise"] is False

    def test_categorize_budget(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("budget_musd", DoubleType(), True),
        ])
        data = [
            (1, 0.5),    # Micro
            (2, 5.0),    # Low
            (3, 25.0),   # Medium
            (4, 75.0),   # High
            (5, 200.0),  # Blockbuster
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.categorize_budget(df)

        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["budget_category"] == "Micro"
        assert rows[2]["budget_category"] == "Low"
        assert rows[3]["budget_category"] == "Medium"
        assert rows[4]["budget_category"] == "High"
        assert rows[5]["budget_category"] == "Blockbuster"

    def test_categorize_rating(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("vote_average", DoubleType(), True),
        ])
        data = [
            (1, 2.0),   # Poor
            (2, 4.5),   # Below Average
            (3, 6.0),   # Average
            (4, 7.2),   # Good
            (5, 8.0),   # Very Good
            (6, 9.0),   # Excellent
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.categorize_rating(df)

        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["rating_category"] == "Poor"
        assert rows[2]["rating_category"] == "Below Average"
        assert rows[3]["rating_category"] == "Average"
        assert rows[4]["rating_category"] == "Good"
        assert rows[5]["rating_category"] == "Very Good"
        assert rows[6]["rating_category"] == "Excellent"

    def test_add_decade(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("release_year", IntegerType(), True),
        ])
        data = [
            (1, 2023),
            (2, 1999),
            (3, 2010),
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.add_decade(df)

        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["decade"] == 2020
        assert rows[2]["decade"] == 1990
        assert rows[3]["decade"] == 2010

    def test_adjust_vote_average_zeroes(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
        ])
        data = [
            (1, 7.5, 100),
            (2, 5.0, 0),  # vote_count 0 → vote_average should be None
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.adjust_vote_average(df)

        rows = {r["id"]: r for r in result.collect()}
        assert rows[1]["vote_average"] == 7.5
        assert rows[2]["vote_average"] is None


@pytest.mark.unit
class TestDataTransformerAdvanced:
    """Advanced transformation tests"""

    def test_calculate_popularity_score(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("vote_average", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("popularity", DoubleType(), True),
        ])
        data = [(1, 8.0, 1000, 50.0)]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.calculate_popularity_score(df)

        row = result.collect()[0]
        assert row["popularity_score"] is not None
        assert row["popularity_score"] > 0
        # Intermediate columns should be dropped
        assert "norm_rating" not in result.columns
        assert "log_votes" not in result.columns

    def test_reorder_columns(self, spark_session, test_config):
        """Reorder should keep all columns, prioritising the defined order."""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("release_year", IntegerType(), True),
            StructField("extra_col", StringType(), True),
        ])
        data = [(1, "Movie", 2023, "extra")]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.reorder_columns(df)

        # id should come first per the defined order
        assert result.columns[0] == "id"
        assert "extra_col" in result.columns

    def test_reset_index(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        data = [(1, "A"), (2, "B"), (3, "C")]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.reset_index(df)

        assert "index" in result.columns
        assert result.count() == 3


@pytest.mark.unit
class TestDataTransformerPipeline:
    """End-to-end transform pipeline test"""

    def test_full_transform_pipeline(self, spark_session, test_config):
        """Run the complete transform() method on minimal data."""
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("tagline", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("genres", StringType(), True),
            StructField("belongs_to_collection", StringType(), True),
            StructField("original_language", StringType(), True),
            StructField("budget", DoubleType(), True),
            StructField("revenue", DoubleType(), True),
            StructField("vote_count", IntegerType(), True),
            StructField("vote_average", DoubleType(), True),
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
            StructField("production_companies", StringType(), True),
            StructField("production_countries", StringType(), True),
        ])
        data = [
            (1, "Movie A", "Tag", "2023-06-15", "Action|Drama", "Collection X",
             "en", 100_000_000.0, 350_000_000.0, 800, 7.2, 45.0, 130,
             "Overview", "English", "/p.jpg", "/b.jpg",
             "Actor1|Actor2", 2, "Dir1", 10, "Studio1", "US"),
        ]
        df = spark_session.createDataFrame(data, schema)

        transformer = DataTransformer(test_config)
        result = transformer.transform(df)

        assert result.count() == 1
        cols = result.columns
        assert "budget_musd" in cols
        assert "revenue_musd" in cols
        assert "profit_musd" in cols
        assert "roi" in cols
        assert "release_year" in cols
        assert "decade" in cols
        assert "has_franchise" in cols
        assert "budget_category" in cols
        assert "rating_category" in cols
        assert "index" in cols
