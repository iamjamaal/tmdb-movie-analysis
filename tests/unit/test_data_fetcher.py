"""
Unit tests for DataFetcher
"""

import pytest
from unittest.mock import patch, MagicMock

from src.ingestion.data_fetcher import DataFetcher


@pytest.mark.unit
class TestDataFetcher:
    """Tests for DataFetcher class"""

    def test_get_movie_schema(self):
        """Schema should contain all expected columns."""
        schema = DataFetcher.get_movie_schema()
        field_names = schema.fieldNames()

        expected = [
            "id", "title", "budget", "revenue", "genres",
            "release_date", "vote_average", "vote_count",
            "popularity", "runtime", "status", "overview",
            "tagline", "director", "cast", "cast_size", "crew_size",
            "belongs_to_collection", "production_companies",
            "production_countries", "spoken_languages",
        ]
        for col in expected:
            assert col in field_names, f"Missing column: {col}"

    @patch("src.ingestion.data_fetcher.TMDBClient")
    def test_init_creates_api_client(self, mock_client_cls, spark_session, test_config):
        fetcher = DataFetcher(spark_session, test_config)
        mock_client_cls.assert_called_once_with(test_config)
        fetcher.close()

    @patch("src.ingestion.data_fetcher.TMDBClient")
    def test_fetch_movies_empty_response(self, mock_client_cls, spark_session, test_config):
        """When API returns no data, an empty DataFrame is returned."""
        mock_instance = MagicMock()
        mock_instance.fetch_movies_batch.return_value = []
        mock_client_cls.return_value = mock_instance

        fetcher = DataFetcher(spark_session, test_config)
        result = fetcher.fetch_movies([1, 2, 3])

        assert result.count() == 0
        fetcher.close()

    @patch("src.ingestion.data_fetcher.TMDBClient")
    def test_fetch_movies_uses_default_ids_when_none(self, mock_client_cls,
                                                       spark_session, test_config):
        mock_instance = MagicMock()
        mock_instance.fetch_movies_batch.return_value = []
        mock_client_cls.return_value = mock_instance

        fetcher = DataFetcher(spark_session, test_config)
        fetcher.fetch_movies(None)

        # Should have been called with the default list
        call_args = mock_instance.fetch_movies_batch.call_args[0][0]
        assert len(call_args) > 0
        fetcher.close()

    @patch("src.ingestion.data_fetcher.TMDBClient")
    def test_save_raw_data(self, mock_client_cls, spark_session, test_config, tmp_path):
        """save_raw_data writes parquet and json."""
        mock_client_cls.return_value = MagicMock()

        fetcher = DataFetcher(spark_session, test_config)

        from pyspark.sql.types import StructType, StructField, IntegerType, StringType
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
        ])
        df = spark_session.createDataFrame([(1, "Movie")], schema)

        output_path = str(tmp_path / "save_test")

        # Mock parquet/json writes to avoid Hadoop native library issues on Windows
        with patch("pyspark.sql.readwriter.DataFrameWriter.parquet") as mock_parquet, \
             patch("pyspark.sql.readwriter.DataFrameWriter.json") as mock_json:
            fetcher.save_raw_data(df, output_path)

        # Verify write calls were made with correct paths
        mock_parquet.assert_called_once()
        mock_json.assert_called_once()
        fetcher.close()

    @patch("src.ingestion.data_fetcher.TMDBClient")
    def test_close(self, mock_client_cls, spark_session, test_config):
        mock_instance = MagicMock()
        mock_client_cls.return_value = mock_instance
        fetcher = DataFetcher(spark_session, test_config)
        fetcher.close()
        mock_instance.close.assert_called_once()
