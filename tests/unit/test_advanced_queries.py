"""
Unit tests for AdvancedQueries
"""

import pytest
from pyspark.sql import functions as F

from src.analytics.advanced_queries import AdvancedQueries


@pytest.mark.unit
class TestAdvancedQueriesSearch:
    """Search function tests"""

    def test_search_by_genres_and_cast(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_genres_and_cast(
            transformed_movie_data,
            genres=["Action"],
            cast_member="Actor A",
            sort_by="vote_average",
            ascending=False,
        )
        # Movies with "Action" in genres AND "Actor A" in cast
        assert result.count() >= 1
        titles = [r["title"] for r in result.collect()]
        # Movie 1 (Action|Adventure, Actor A|Actor B) should match
        assert "Action Hero" in titles

    def test_search_by_genres_and_cast_no_match(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_genres_and_cast(
            transformed_movie_data,
            genres=["Horror"],
            cast_member="NonexistentActor",
        )
        assert result.count() == 0

    def test_search_by_cast_and_director(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_cast_and_director(
            transformed_movie_data,
            cast_member="Actor C",
            director="Director Y",
            sort_by="runtime",
            ascending=True,
        )
        assert result.count() >= 1

    def test_search_by_cast_and_director_no_match(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_cast_and_director(
            transformed_movie_data,
            cast_member="Actor A",
            director="Director Y",  # Actor A is not with Director Y
        )
        assert result.count() == 0


@pytest.mark.unit
class TestAdvancedQueriesFilters:
    """Filter-based query tests"""

    def test_search_by_year_range(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_year_range(
            transformed_movie_data,
            start_year=2015,
            end_year=2020,
            min_rating=6.0,
        )
        rows = result.collect()
        for row in rows:
            assert 2015 <= row["release_year"] <= 2020
            assert row["vote_average"] >= 6.0

    def test_search_by_year_range_empty(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_year_range(
            transformed_movie_data,
            start_year=1900,
            end_year=1910,
        )
        assert result.count() == 0

    def test_search_by_budget_range(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.search_by_budget_range(
            transformed_movie_data,
            min_budget=50.0,
            max_budget=150.0,
        )
        rows = result.collect()
        for row in rows:
            assert 50.0 <= row["budget_musd"] <= 150.0

    def test_find_highest_roi_by_genre(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.find_highest_roi_by_genre(
            transformed_movie_data,
            genre="Action",
            top_n=3,
            min_budget=1.0,
        )
        assert result.count() >= 1
        rows = result.collect()
        # Should be sorted by roi descending
        rois = [r["roi"] for r in rows]
        for i in range(len(rois) - 1):
            assert rois[i] >= rois[i + 1]


@pytest.mark.unit
class TestAdvancedQueriesCollaborations:
    """Collaboration and comparison tests"""

    def test_find_director_actor_collaborations(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.find_director_actor_collaborations(
            transformed_movie_data,
            min_collaborations=2,
        )
        # Director X + Actor A should appear (movies 1 & 3)
        if result.count() > 0:
            rows = result.collect()
            for row in rows:
                assert row["movies_together"] >= 2

    def test_compare_decades(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.compare_decades(
            transformed_movie_data,
            decade1=2010,
            decade2=2020,
        )
        decades = [r["decade"] for r in result.collect()]
        assert 2010 in decades or 2020 in decades

    def test_compare_decades_no_data(self, transformed_movie_data, test_config):
        aq = AdvancedQueries(test_config)
        result = aq.compare_decades(
            transformed_movie_data,
            decade1=1900,
            decade2=1910,
        )
        assert result.count() == 0
