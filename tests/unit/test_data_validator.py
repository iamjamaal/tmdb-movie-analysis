"""
Unit tests for DataValidator
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

from src.processing.data_validator import DataValidator


@pytest.mark.unit
class TestDataValidatorSchema:
    """Schema validation tests"""

    def test_validate_schema_pass(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "Movie")], schema)

        validator = DataValidator(test_config)
        is_valid, errors = validator.validate_schema(df, schema)
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_schema_missing_column(self, spark_session, test_config):
        actual_schema = StructType([
            StructField("id", IntegerType(), False),
        ])
        expected_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1,)], actual_schema)

        validator = DataValidator(test_config)
        is_valid, errors = validator.validate_schema(df, expected_schema)
        assert is_valid is False
        assert any("Missing" in e for e in errors)

    def test_validate_schema_extra_column(self, spark_session, test_config):
        actual_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("extra", StringType(), True),
        ])
        expected_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "M", "x")], actual_schema)

        validator = DataValidator(test_config)
        is_valid, errors = validator.validate_schema(df, expected_schema)
        assert is_valid is False
        assert any("Extra" in e for e in errors)

    def test_validate_schema_type_mismatch(self, spark_session, test_config):
        actual_schema = StructType([
            StructField("id", StringType(), False),  # string instead of int
        ])
        expected_schema = StructType([
            StructField("id", IntegerType(), False),
        ])
        df = spark_session.createDataFrame([("1",)], actual_schema)

        validator = DataValidator(test_config)
        is_valid, errors = validator.validate_schema(df, expected_schema)
        assert is_valid is False
        assert any("Type mismatch" in e for e in errors)


@pytest.mark.unit
class TestDataValidatorCompleteness:
    """Completeness validation tests"""

    def test_completeness_all_present(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "A"), (2, "B")], schema)

        validator = DataValidator(test_config)
        report = validator.validate_completeness(df)

        assert report['total_rows'] == 2
        assert report['column_completeness']['id']['completeness_percentage'] == 100.0
        assert report['column_completeness']['title']['completeness_percentage'] == 100.0

    def test_completeness_with_nulls(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "A"), (2, None), (3, None)], schema)

        validator = DataValidator(test_config)
        report = validator.validate_completeness(df)

        title_completeness = report['column_completeness']['title']
        assert title_completeness['null_count'] == 2
        assert abs(title_completeness['completeness_percentage'] - 33.33) < 0.1

    def test_completeness_empty_df(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
        ])
        df = spark_session.createDataFrame([], schema)

        validator = DataValidator(test_config)
        report = validator.validate_completeness(df)
        assert report['total_rows'] == 0


@pytest.mark.unit
class TestDataValidatorUniqueness:
    """Uniqueness validation tests"""

    def test_uniqueness_all_unique(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "A"), (2, "B")], schema)

        validator = DataValidator(test_config)
        report = validator.validate_uniqueness(df, ['id'])

        assert report['id']['is_unique'] is True
        assert report['id']['duplicate_count'] == 0

    def test_uniqueness_with_duplicates(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "A"), (1, "B"), (2, "C")], schema)

        validator = DataValidator(test_config)
        report = validator.validate_uniqueness(df, ['id'])

        assert report['id']['is_unique'] is False
        assert report['id']['duplicate_count'] == 1


@pytest.mark.unit
class TestDataValidatorRanges:
    """Range validation tests"""

    def test_ranges_all_valid(self, spark_session, test_config):
        schema = StructType([
            StructField("vote_average", DoubleType(), True),
        ])
        df = spark_session.createDataFrame([(7.5,), (8.0,), (6.0,)], schema)

        config = dict(test_config)
        config['validation'] = {
            'ranges': {
                'vote_average': {'min': 0, 'max': 10}
            }
        }
        validator = DataValidator(config)
        report = validator.validate_ranges(df)

        assert report['vote_average']['is_valid'] is True
        assert report['vote_average']['violations_below_min'] == 0
        assert report['vote_average']['violations_above_max'] == 0

    def test_ranges_with_violations(self, spark_session, test_config):
        schema = StructType([
            StructField("vote_average", DoubleType(), True),
        ])
        df = spark_session.createDataFrame([(-1.0,), (11.0,), (7.0,)], schema)

        config = dict(test_config)
        config['validation'] = {
            'ranges': {
                'vote_average': {'min': 0, 'max': 10}
            }
        }
        validator = DataValidator(config)
        report = validator.validate_ranges(df)

        assert report['vote_average']['is_valid'] is False
        assert report['vote_average']['violations_below_min'] == 1
        assert report['vote_average']['violations_above_max'] == 1


@pytest.mark.unit
class TestDataValidatorBusinessRules:
    """Business rule validation tests"""

    def test_vote_average_range_valid(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("vote_average", DoubleType(), True),
        ])
        df = spark_session.createDataFrame([(1, 7.5), (2, 3.0)], schema)

        validator = DataValidator(test_config)
        report = validator.validate_business_rules(df)

        assert report['vote_average_range']['is_valid'] is True

    def test_vote_average_range_invalid(self, spark_session, test_config):
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("vote_average", DoubleType(), True),
        ])
        df = spark_session.createDataFrame([(1, 12.0)], schema)

        validator = DataValidator(test_config)
        report = validator.validate_business_rules(df)

        assert report['vote_average_range']['is_valid'] is False
        assert report['vote_average_range']['violations'] == 1


@pytest.mark.unit
class TestDataValidatorReport:
    """Comprehensive report generation test"""

    def test_generate_validation_report(self, transformed_movie_data, test_config):
        validator = DataValidator(test_config)
        report = validator.generate_validation_report(transformed_movie_data)

        assert report['total_records'] == 5
        assert 'completeness' in report
        assert 'uniqueness' in report
        assert 'ranges' in report
        assert 'business_rules' in report

    def test_log_data_quality_issues(self, transformed_movie_data, test_config):
        """Should not raise even if no issues found."""
        validator = DataValidator(test_config)
        validator.log_data_quality_issues(transformed_movie_data)  # no assert, just run
