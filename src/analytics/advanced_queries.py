"""
Advanced Queries - Complex search and filtering operations
"""

import logging
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class AdvancedQueries:
    """
    Execute advanced search queries on movie data
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize AdvancedQueries
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.queries_config = config.get('queries', {})
        logger.info("AdvancedQueries initialized")
    
    def search_by_genres_and_cast(
        self,
        df: DataFrame,
        genres: List[str],
        cast_member: str,
        sort_by: str = "vote_average",
        ascending: bool = False
    ) -> DataFrame:
        """
        Search for movies by genres and cast member
        
        Args:
            df: Input DataFrame
            genres: List of genres to search for
            cast_member: Cast member name
            sort_by: Column to sort by
            ascending: Sort order
            
        Returns:
            Filtered DataFrame
        """
        logger.info(f"Searching for {genres} movies starring {cast_member}")
        
        # Build genre filter condition
        genre_conditions = [F.col("genres").contains(genre) for genre in genres]
        combined_genre_condition = genre_conditions[0]
        for condition in genre_conditions[1:]:
            combined_genre_condition = combined_genre_condition & condition
        
        # Filter by genres and cast
        result = df.filter(
            combined_genre_condition &
            F.col("cast").contains(cast_member)
        ).orderBy(F.col(sort_by).asc() if ascending else F.col(sort_by).desc())
        
        count = result.count()
        logger.info(f"Found {count} movies matching criteria")
        
        return result
    
    def search_by_cast_and_director(
        self,
        df: DataFrame,
        cast_member: str,
        director: str,
        sort_by: str = "runtime",
        ascending: bool = True
    ) -> DataFrame:
        """
        Search for movies by cast member and director
        
        Args:
            df: Input DataFrame
            cast_member: Cast member name
            director: Director name
            sort_by: Column to sort by
            ascending: Sort order
            
        Returns:
            Filtered DataFrame
        """
        logger.info(f"Searching for movies starring {cast_member} directed by {director}")
        
        result = df.filter(
            F.col("cast").contains(cast_member) &
            (F.col("director") == director)
        ).orderBy(F.col(sort_by).asc() if ascending else F.col(sort_by).desc())
        
        count = result.count()
        logger.info(f"Found {count} movies matching criteria")
        
        return result
    
    def execute_search_1(self, df: DataFrame) -> DataFrame:
        """
        Execute Search 1: Best-rated Science Fiction Action movies starring Bruce Willis
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        search_config = self.queries_config.get('search_1', {})
        
        genres = search_config.get('genres', ["Science Fiction", "Action"])
        cast = search_config.get('cast', "Bruce Willis")
        sort_by = search_config.get('sort_by', "vote_average")
        ascending = search_config.get('ascending', False)
        
        logger.info("Executing Search 1: Science Fiction Action movies with Bruce Willis")
        
        result = self.search_by_genres_and_cast(
            df=df,
            genres=genres,
            cast_member=cast,
            sort_by=sort_by,
            ascending=ascending
        )
        
        # Select relevant columns for output
        return result.select(
            "id", "title", "release_year", "genres",
            "vote_average", "vote_count", "popularity",
            "budget_musd", "revenue_musd", "profit_musd", "roi",
            "cast", "director", "runtime"
        )
    
    def execute_search_2(self, df: DataFrame) -> DataFrame:
        """
        Execute Search 2: Movies starring Uma Thurman directed by Quentin Tarantino
        
        Args:
            df: Input DataFrame
            
        Returns:
            Filtered DataFrame
        """
        search_config = self.queries_config.get('search_2', {})
        
        cast = search_config.get('cast', "Uma Thurman")
        director = search_config.get('director', "Quentin Tarantino")
        sort_by = search_config.get('sort_by', "runtime")
        ascending = search_config.get('ascending', True)
        
        logger.info("Executing Search 2: Uma Thurman movies directed by Quentin Tarantino")
        
        result = self.search_by_cast_and_director(
            df=df,
            cast_member=cast,
            director=director,
            sort_by=sort_by,
            ascending=ascending
        )
        
        # Select relevant columns for output
        return result.select(
            "id", "title", "release_year", "genres",
            "runtime", "vote_average", "vote_count",
            "budget_musd", "revenue_musd", "profit_musd",
            "cast", "director", "tagline"
        )
    
    def search_by_year_range(
        self,
        df: DataFrame,
        start_year: int,
        end_year: int,
        min_rating: float = 0.0
    ) -> DataFrame:
        """
        Search for movies within a year range and minimum rating
        
        Args:
            df: Input DataFrame
            start_year: Start year (inclusive)
            end_year: End year (inclusive)
            min_rating: Minimum vote average
            
        Returns:
            Filtered DataFrame
        """
        logger.info(f"Searching for movies from {start_year} to {end_year} with rating >= {min_rating}")
        
        result = df.filter(
            (F.col("release_year") >= start_year) &
            (F.col("release_year") <= end_year) &
            (F.col("vote_average") >= min_rating)
        )
        
        return result
    
    def search_by_budget_range(
        self,
        df: DataFrame,
        min_budget: float,
        max_budget: float
    ) -> DataFrame:
        """
        Search for movies within a budget range
        
        Args:
            df: Input DataFrame
            min_budget: Minimum budget in million USD
            max_budget: Maximum budget in million USD
            
        Returns:
            Filtered DataFrame
        """
        logger.info(f"Searching for movies with budget between ${min_budget}M and ${max_budget}M")
        
        result = df.filter(
            (F.col("budget_musd") >= min_budget) &
            (F.col("budget_musd") <= max_budget)
        )
        
        return result
    
    def find_highest_roi_by_genre(
        self,
        df: DataFrame,
        genre: str,
        top_n: int = 10,
        min_budget: float = 1.0
    ) -> DataFrame:
        """
        Find highest ROI movies for a specific genre
        
        Args:
            df: Input DataFrame
            genre: Genre name
            top_n: Number of top movies to return
            min_budget: Minimum budget threshold
            
        Returns:
            Top N movies by ROI
        """
        logger.info(f"Finding top {top_n} ROI movies in {genre} genre")
        
        result = df.filter(
            F.col("genres").contains(genre) &
            (F.col("budget_musd") >= min_budget) &
            F.col("roi").isNotNull()
        ).orderBy(F.col("roi").desc()).limit(top_n)
        
        return result
    
    def find_director_actor_collaborations(
        self,
        df: DataFrame,
        min_collaborations: int = 2
    ) -> DataFrame:
        """
        Find frequent director-actor collaborations
        
        Args:
            df: Input DataFrame
            min_collaborations: Minimum number of movies together
            
        Returns:
            DataFrame with collaborations
        """
        logger.info(f"Finding director-actor collaborations (>= {min_collaborations} movies)")
        
        # Explode cast to get individual actors
        df_exploded = df.withColumn("actor", F.explode(F.split(F.col("cast"), "\\|")))
        
        # Count collaborations
        collaborations = df_exploded.groupBy("director", "actor").agg(
            F.count("*").alias("movies_together"),
            F.collect_list("title").alias("movies"),
            F.mean("vote_average").alias("avg_rating"),
            F.sum("revenue_musd").alias("total_revenue")
        ).filter(F.col("movies_together") >= min_collaborations) \
          .orderBy("movies_together", ascending=False)
        
        return collaborations
    
    def compare_decades(
        self,
        df: DataFrame,
        decade1: int,
        decade2: int
    ) -> DataFrame:
        """
        Compare movie metrics between two decades
        
        Args:
            df: Input DataFrame
            decade1: First decade (e.g., 1990)
            decade2: Second decade (e.g., 2000)
            
        Returns:
            Comparison DataFrame
        """
        logger.info(f"Comparing decades {decade1} vs {decade2}")
        
        comparison = df.filter(
            (F.col("decade") == decade1) | (F.col("decade") == decade2)
        ).groupBy("decade").agg(
            F.count("*").alias("movie_count"),
            F.mean("revenue_musd").alias("avg_revenue"),
            F.mean("budget_musd").alias("avg_budget"),
            F.mean("roi").alias("avg_roi"),
            F.mean("vote_average").alias("avg_rating"),
            F.mean("popularity").alias("avg_popularity")
        )
        
        return comparison