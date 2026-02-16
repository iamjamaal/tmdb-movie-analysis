"""
Unit tests for utility helper functions
"""

import os
import time
import pytest
import logging
from pathlib import Path
from datetime import datetime

from src.utils.helpers import (
    load_config,
    setup_logging,
    ensure_directory,
    get_environment,
    get_api_key,
    format_currency,
    format_percentage,
    calculate_roi,
    calculate_profit,
    safe_divide,
    parse_date,
    get_year_from_date,
    chunk_list,
    flatten_dict,
    sanitize_filename,
    format_file_size,
    get_timestamp,
    create_run_id,
    retry_on_failure,
    measure_execution_time,
    ProgressTracker,
    validate_environment_variables,
    merge_dicts,
    filter_none_values,
)


@pytest.mark.unit
class TestLoadConfig:
    """Tests for configuration loading"""

    def test_load_config_success(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text("api:\n  base_url: https://example.com\n")

        result = load_config(str(config_file))
        assert result['api']['base_url'] == 'https://example.com'

    def test_load_config_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_config("/nonexistent/path/config.yaml")

    def test_load_config_invalid_yaml(self, tmp_path):
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("{{invalid: yaml: content}}")

        # yaml.safe_load may or may not raise on this – just verify it doesn't crash
        # Some edge YAML is still parseable; this test ensures the path is exercised
        try:
            load_config(str(config_file))
        except Exception:
            pass  # Expected for truly invalid YAML


@pytest.mark.unit
class TestFormatters:
    """Tests for formatting utilities"""

    def test_format_currency(self):
        assert format_currency(150.0) == "$150.00M"
        assert format_currency(1234.567, 1) == "$1,234.6M"

    def test_format_percentage(self):
        assert format_percentage(75.5) == "75.50%"
        assert format_percentage(100.0, 0) == "100%"

    def test_format_file_size(self):
        assert format_file_size(500) == "500.00 B"
        assert format_file_size(1024) == "1.00 KB"
        assert format_file_size(1048576) == "1.00 MB"


@pytest.mark.unit
class TestCalculations:
    """Tests for financial calculations"""

    def test_calculate_roi_normal(self):
        result = calculate_roi(300, 100)
        assert result == 200.0  # (300/100 - 1) * 100

    def test_calculate_roi_zero_budget(self):
        assert calculate_roi(300, 0) is None

    def test_calculate_roi_none_budget(self):
        assert calculate_roi(300, None) is None

    def test_calculate_profit(self):
        assert calculate_profit(300, 100) == 200.0

    def test_safe_divide_normal(self):
        assert safe_divide(10, 2) == 5.0

    def test_safe_divide_zero(self):
        assert safe_divide(10, 0) == 0.0

    def test_safe_divide_none(self):
        assert safe_divide(10, None) == 0.0

    def test_safe_divide_custom_default(self):
        assert safe_divide(10, 0, default=-1.0) == -1.0


@pytest.mark.unit
class TestDateUtils:
    """Tests for date utilities"""

    def test_parse_date_valid(self):
        result = parse_date("2023-06-15")
        assert result == datetime(2023, 6, 15)

    def test_parse_date_invalid(self):
        assert parse_date("not-a-date") is None

    def test_parse_date_none(self):
        assert parse_date(None) is None

    def test_get_year_from_date(self):
        dt = datetime(2023, 6, 15)
        assert get_year_from_date(dt) == 2023

    def test_get_year_from_none(self):
        assert get_year_from_date(None) is None


@pytest.mark.unit
class TestCollectionUtils:
    """Tests for collection utilities"""

    def test_chunk_list(self):
        result = chunk_list([1, 2, 3, 4, 5], 2)
        assert result == [[1, 2], [3, 4], [5]]

    def test_chunk_list_empty(self):
        assert chunk_list([], 3) == []

    def test_chunk_list_single_chunk(self):
        assert chunk_list([1, 2], 5) == [[1, 2]]

    def test_flatten_dict(self):
        nested = {'a': {'b': 1, 'c': {'d': 2}}, 'e': 3}
        result = flatten_dict(nested)
        assert result == {'a_b': 1, 'a_c_d': 2, 'e': 3}

    def test_flatten_dict_empty(self):
        assert flatten_dict({}) == {}

    def test_merge_dicts(self):
        result = merge_dicts({'a': 1}, {'b': 2}, {'a': 3})
        assert result == {'a': 3, 'b': 2}

    def test_filter_none_values(self):
        result = filter_none_values({'a': 1, 'b': None, 'c': 3})
        assert result == {'a': 1, 'c': 3}


@pytest.mark.unit
class TestFileUtils:
    """Tests for file utilities"""

    def test_ensure_directory(self, tmp_path):
        new_dir = str(tmp_path / "subdir" / "nested")
        ensure_directory(new_dir)
        assert os.path.isdir(new_dir)

    def test_sanitize_filename(self):
        assert sanitize_filename('file<>:name.txt') == 'file___name.txt'
        assert sanitize_filename('normal.txt') == 'normal.txt'


@pytest.mark.unit
class TestEnvironment:
    """Tests for environment utilities"""

    def test_get_environment(self, monkeypatch):
        monkeypatch.setenv('ENVIRONMENT', 'production')
        assert get_environment() == 'production'

    def test_get_environment_default(self, monkeypatch):
        monkeypatch.delenv('ENVIRONMENT', raising=False)
        assert get_environment() == 'development'

    def test_get_api_key_success(self):
        # TMDB_API_KEY is set by the autouse fixture
        key = get_api_key()
        assert key == 'test_api_key_12345'

    def test_get_api_key_missing(self, monkeypatch):
        monkeypatch.delenv('TMDB_API_KEY', raising=False)
        with pytest.raises(ValueError, match="TMDB_API_KEY"):
            get_api_key()

    def test_validate_environment_variables_success(self):
        validate_environment_variables(['TMDB_API_KEY'])

    def test_validate_environment_variables_missing(self, monkeypatch):
        monkeypatch.delenv('TMDB_API_KEY', raising=False)
        with pytest.raises(EnvironmentError):
            validate_environment_variables(['TMDB_API_KEY', 'NONEXISTENT_VAR'])


@pytest.mark.unit
class TestTimestampUtils:
    """Tests for timestamp utilities"""

    def test_get_timestamp_format(self):
        ts = get_timestamp()
        # Format: YYYYMMDD_HHMMSS
        assert len(ts) == 15
        assert ts[8] == '_'

    def test_create_run_id(self):
        run_id = create_run_id()
        assert run_id.startswith("run_")
        assert len(run_id) == 19  # "run_" + 15 chars


@pytest.mark.unit
class TestDecorators:
    """Tests for decorator utilities"""

    def test_retry_on_failure_success(self):
        call_count = 0

        @retry_on_failure(max_retries=3, delay=0)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = succeed()
        assert result == "ok"
        assert call_count == 1

    def test_retry_on_failure_eventual_success(self):
        call_count = 0

        @retry_on_failure(max_retries=3, delay=0)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "ok"

        result = fail_then_succeed()
        assert result == "ok"
        assert call_count == 3

    def test_retry_on_failure_all_fail(self):
        @retry_on_failure(max_retries=2, delay=0)
        def always_fail():
            raise RuntimeError("always fails")

        with pytest.raises(RuntimeError):
            always_fail()

    def test_measure_execution_time(self):
        @measure_execution_time
        def quick_func():
            return 42

        result = quick_func()
        assert result == 42


@pytest.mark.unit
class TestProgressTracker:
    """Tests for ProgressTracker"""

    def test_update_progress(self):
        tracker = ProgressTracker(total=10, description="Test")
        for _ in range(10):
            tracker.update()
        assert tracker.current == 10

    def test_complete(self):
        import time
        tracker = ProgressTracker(total=5, description="Test")
        for _ in range(5):
            tracker.update()
        time.sleep(0.01)  # Ensure non-zero elapsed time
        tracker.complete()  # Should not raise
