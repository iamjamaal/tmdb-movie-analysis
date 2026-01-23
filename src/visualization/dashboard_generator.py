"""
Dashboard Generator - Create visualizations and analytics dashboards
"""

import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DashboardGenerator:
    """Generate visualizations and dashboards from processed data"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.output_dir = Path(config.get('paths', {}).get('output', './data/output'))
        self.viz_dir = self.output_dir / 'visualizations'
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        
        # Set style
        sns.set_style("whitegrid")
        sns.set_palette("husl")
        
    def plot_revenue_vs_budget(
        self, 
        df: DataFrame,
        output_file: str = "revenue_vs_budget.png"
    ) -> str:
        """Plot revenue vs budget scatter plot"""
        logger.info("Generating revenue vs budget visualization")
        
        # Convert to pandas for plotting
        plot_data = df.select(
            'budget_musd', 'revenue_musd', 'title'
        ).filter(
            (F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0)
        ).toPandas()
        
        plt.figure(figsize=(12, 8))
        plt.scatter(
            plot_data['budget_musd'],
            plot_data['revenue_musd'],
            alpha=0.6,
            s=100
        )
        
        # Add diagonal line (break-even point)
        max_val = max(plot_data['budget_musd'].max(), plot_data['revenue_musd'].max())
        plt.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, label='Break-even')
        
        plt.xlabel('Budget (Million USD)', fontsize=12)
        plt.ylabel('Revenue (Million USD)', fontsize=12)
        plt.title('Movie Revenue vs Budget', fontsize=14, fontweight='bold')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved revenue vs budget plot to {output_path}")
        return str(output_path)
    
    def plot_genre_distribution(
        self,
        df: DataFrame,
        output_file: str = "genre_distribution.png"
    ) -> str:
        """Plot genre distribution"""
        logger.info("Generating genre distribution visualization")
        
        # Explode and count genres
        genre_counts = df.withColumn(
            'genre', F.explode(F.split('genres', '\\|'))
        ).groupBy('genre').count().orderBy(F.desc('count')).limit(15).toPandas()
        
        plt.figure(figsize=(14, 8))
        plt.barh(range(len(genre_counts)), genre_counts['count'])
        plt.yticks(range(len(genre_counts)), genre_counts['genre'])
        plt.xlabel('Number of Movies', fontsize=12)
        plt.ylabel('Genre', fontsize=12)
        plt.title('Top 15 Movie Genres Distribution', fontsize=14, fontweight='bold')
        plt.grid(True, axis='x', alpha=0.3)
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved genre distribution plot to {output_path}")
        return str(output_path)
    
    def plot_yearly_trends(
        self,
        df: DataFrame,
        output_file: str = "yearly_trends.png"
    ) -> str:
        """Plot yearly trends in movie metrics"""
        logger.info("Generating yearly trends visualization")
        
        yearly_data = df.withColumn('year', F.year('release_date')).groupBy('year').agg(
            F.count('*').alias('movie_count'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating')
        ).orderBy('year').toPandas()
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # Movie count trend
        axes[0, 0].plot(yearly_data['year'], yearly_data['movie_count'], marker='o')
        axes[0, 0].set_title('Movies Released per Year', fontweight='bold')
        axes[0, 0].set_xlabel('Year')
        axes[0, 0].set_ylabel('Number of Movies')
        axes[0, 0].grid(True, alpha=0.3)
        
        # Average revenue trend
        axes[0, 1].plot(yearly_data['year'], yearly_data['avg_revenue'], marker='o', color='green')
        axes[0, 1].set_title('Average Revenue Trend', fontweight='bold')
        axes[0, 1].set_xlabel('Year')
        axes[0, 1].set_ylabel('Average Revenue (Million USD)')
        axes[0, 1].grid(True, alpha=0.3)
        
        # Average budget trend
        axes[1, 0].plot(yearly_data['year'], yearly_data['avg_budget'], marker='o', color='orange')
        axes[1, 0].set_title('Average Budget Trend', fontweight='bold')
        axes[1, 0].set_xlabel('Year')
        axes[1, 0].set_ylabel('Average Budget (Million USD)')
        axes[1, 0].grid(True, alpha=0.3)
        
        # Average rating trend
        axes[1, 1].plot(yearly_data['year'], yearly_data['avg_rating'], marker='o', color='red')
        axes[1, 1].set_title('Average Rating Trend', fontweight='bold')
        axes[1, 1].set_xlabel('Year')
        axes[1, 1].set_ylabel('Average Rating')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved yearly trends plot to {output_path}")
        return str(output_path)
    
    def plot_roi_distribution(
        self,
        df: DataFrame,
        output_file: str = "roi_distribution.png"
    ) -> str:
        """Plot ROI distribution by genre"""
        logger.info("Generating ROI distribution visualization")
        
        # Calculate ROI and explode genres
        roi_data = df.filter(
            (F.col('budget_musd') > 10) & (F.col('revenue_musd') > 0)
        ).withColumn(
            'roi', (F.col('revenue_musd') / F.col('budget_musd') - 1) * 100
        ).withColumn(
            'genre', F.explode(F.split('genres', '\\|'))
        ).select('genre', 'roi')
        
        # Get top genres by count
        top_genres = roi_data.groupBy('genre').count().orderBy(
            F.desc('count')
        ).limit(10).select('genre').toPandas()['genre'].tolist()
        
        # Filter to top genres
        plot_data = roi_data.filter(
            F.col('genre').isin(top_genres)
        ).toPandas()
        
        plt.figure(figsize=(14, 8))
        sns.boxplot(data=plot_data, x='genre', y='roi')
        plt.xticks(rotation=45, ha='right')
        plt.xlabel('Genre', fontsize=12)
        plt.ylabel('ROI (%)', fontsize=12)
        plt.title('ROI Distribution by Top 10 Genres', fontsize=14, fontweight='bold')
        plt.axhline(y=0, color='r', linestyle='--', alpha=0.5, label='Break-even')
        plt.legend()
        plt.grid(True, axis='y', alpha=0.3)
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved ROI distribution plot to {output_path}")
        return str(output_path)
    
    def plot_rating_vs_popularity(
        self,
        df: DataFrame,
        output_file: str = "rating_vs_popularity.png"
    ) -> str:
        """Plot rating vs popularity correlation"""
        logger.info("Generating rating vs popularity visualization")
        
        plot_data = df.select(
            'vote_average', 'popularity', 'vote_count', 'title'
        ).filter(
            (F.col('vote_count') >= 100)
        ).toPandas()
        
        plt.figure(figsize=(12, 8))
        scatter = plt.scatter(
            plot_data['vote_average'],
            plot_data['popularity'],
            c=plot_data['vote_count'],
            cmap='viridis',
            alpha=0.6,
            s=100
        )
        
        plt.colorbar(scatter, label='Vote Count')
        plt.xlabel('Vote Average', fontsize=12)
        plt.ylabel('Popularity', fontsize=12)
        plt.title('Movie Rating vs Popularity', fontsize=14, fontweight='bold')
        plt.grid(True, alpha=0.3)
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved rating vs popularity plot to {output_path}")
        return str(output_path)
    
    def plot_franchise_comparison(
        self,
        df: DataFrame,
        output_file: str = "franchise_comparison.png"
    ) -> str:
        """Compare franchise vs standalone movies"""
        logger.info("Generating franchise comparison visualization")
        
        comparison_data = df.withColumn(
            'is_franchise',
            F.when(F.col('belongs_to_collection').isNotNull(), 'Franchise').otherwise('Standalone')
        ).groupBy('is_franchise').agg(
            F.count('*').alias('count'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity')
        ).toPandas()
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Average revenue
        axes[0, 0].bar(comparison_data['is_franchise'], comparison_data['avg_revenue'])
        axes[0, 0].set_title('Average Revenue', fontweight='bold')
        axes[0, 0].set_ylabel('Revenue (Million USD)')
        axes[0, 0].grid(True, axis='y', alpha=0.3)
        
        # Average budget
        axes[0, 1].bar(comparison_data['is_franchise'], comparison_data['avg_budget'], color='orange')
        axes[0, 1].set_title('Average Budget', fontweight='bold')
        axes[0, 1].set_ylabel('Budget (Million USD)')
        axes[0, 1].grid(True, axis='y', alpha=0.3)
        
        # Average rating
        axes[1, 0].bar(comparison_data['is_franchise'], comparison_data['avg_rating'], color='green')
        axes[1, 0].set_title('Average Rating', fontweight='bold')
        axes[1, 0].set_ylabel('Rating')
        axes[1, 0].grid(True, axis='y', alpha=0.3)
        
        # Movie count
        axes[1, 1].bar(comparison_data['is_franchise'], comparison_data['count'], color='purple')
        axes[1, 1].set_title('Movie Count', fontweight='bold')
        axes[1, 1].set_ylabel('Count')
        axes[1, 1].grid(True, axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info(f"Saved franchise comparison plot to {output_path}")
        return str(output_path)
    
    def generate_all_visualizations(self, df: DataFrame) -> Dict[str, str]:
        """Generate all standard visualizations"""
        logger.info("Generating all visualizations")
        
        visualizations = {
            'revenue_budget': self.plot_revenue_vs_budget(df),
            'genre_distribution': self.plot_genre_distribution(df),
            'yearly_trends': self.plot_yearly_trends(df),
            'roi_distribution': self.plot_roi_distribution(df),
            'rating_popularity': self.plot_rating_vs_popularity(df),
            'franchise_comparison': self.plot_franchise_comparison(df)
        }
        
        logger.info(f"Generated {len(visualizations)} visualizations")
        return visualizations
    
    def create_summary_report(
        self,
        df: DataFrame,
        kpis: Dict[str, Any],
        output_file: str = "summary_report.html"
    ) -> str:
        """Create HTML summary report"""
        logger.info("Creating summary report")
        
        # Calculate summary statistics
        stats = df.agg(
            F.count('*').alias('total_movies'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('vote_average').alias('avg_rating')
        ).collect()[0]
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>TMDB Movie Analysis Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; }}
                h1 {{ color: #2c3e50; }}
                .metric {{ background: #ecf0f1; padding: 15px; margin: 10px 0; border-radius: 5px; }}
                .metric-value {{ font-size: 24px; color: #27ae60; font-weight: bold; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #3498db; color: white; }}
            </style>
        </head>
        <body>
            <h1>TMDB Movie Data Analysis Report</h1>
            <div class="metric">
                <h3>Total Movies Analyzed</h3>
                <div class="metric-value">{stats['total_movies']}</div>
            </div>
            <div class="metric">
                <h3>Total Revenue</h3>
                <div class="metric-value">${stats['total_revenue']:,.2f}M</div>
            </div>
            <div class="metric">
                <h3>Average Rating</h3>
                <div class="metric-value">{stats['avg_rating']:.2f}/10</div>
            </div>
            <h2>Top Performing Movies</h2>
            <!-- Add KPI tables here -->
        </body>
        </html>
        """
        
        output_path = self.viz_dir / output_file
        with open(output_path, 'w') as f:
            f.write(html_content)
        
        logger.info(f"Saved summary report to {output_path}")
        return str(output_path)