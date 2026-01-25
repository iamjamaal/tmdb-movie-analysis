"""
Dashboard Generator - Comprehensive Visualization Suite
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import json
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

# Optional Plotly integration
try:
    from .plotly.dashboard import PlotlyDashboardGenerator
    _HAS_PLOTLY = True
except Exception:
    PlotlyDashboardGenerator = None  # type: ignore
    _HAS_PLOTLY = False


class DashboardGenerator:
    """Generate comprehensive visualizations and interactive dashboards"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Prefer explicit output base from config (output.base_path), fallback to legacy keys
        output_base = None
        try:
            output_base = config.get('output', {}).get('base_path') if config else None
        except Exception:
            output_base = None
        if output_base:
            self.output_dir = Path(output_base)
        else:
            self.output_dir = Path(config.get('paths', {}).get('output', './data/output'))
        self.viz_dir = self.output_dir / 'visualizations'
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        
        # Set modern style
        plt.style.use('seaborn-v0_8-darkgrid')
        sns.set_palette("husl")
        
        # Color schemes
        self.colors = {
            'primary': '#3498db',
            'success': '#2ecc71',
            'warning': '#f39c12',
            'danger': '#e74c3c',
            'info': '#9b59b6',
            'dark': '#2c3e50',
            'light': '#ecf0f1'
        }
        
    def create_executive_summary_dashboard(
        self,
        df: DataFrame,
        kpis: Dict[str, Any],
        output_file: str = "executive_summary.png"
    ) -> str:
        """Create executive summary dashboard with key metrics"""
        logger.info("Creating executive summary dashboard")
        
        # Calculate summary metrics
        total_movies = df.count()
        total_revenue = df.select(F.sum('revenue_musd')).collect()[0][0] or 0
        avg_rating = df.select(F.avg('vote_average')).collect()[0][0] or 0
        total_budget = df.select(F.sum('budget_musd')).collect()[0][0] or 0
        
        # Calculate ROI
        roi_df = df.filter((F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0))
        avg_roi = roi_df.select(
            F.avg((F.col('revenue_musd') / F.col('budget_musd') - 1) * 100)
        ).collect()[0][0] or 0
        
        # Get top genre
        top_genre = df.withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                     .groupBy('genre').count() \
                     .orderBy(F.desc('count')) \
                     .limit(1).collect()
        top_genre_name = top_genre[0]['genre'] if top_genre else 'N/A'
        
        # Create figure with subplots
        fig = plt.figure(figsize=(20, 12))
        gs = fig.add_gridspec(3, 4, hspace=0.3, wspace=0.3)
        
        # Title
        fig.suptitle('TMDB Movie Data Analysis - Executive Summary', 
                    fontsize=24, fontweight='bold', y=0.98)
        
        # Add subtitle with timestamp
        fig.text(0.5, 0.94, f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M")}',
                ha='center', fontsize=12, style='italic', color='gray')
        
        # === KPI Cards (Top Row) ===
        kpi_data = [
            ('Total Movies', f'{total_movies:,}', self.colors['primary'], '🎬'),
            ('Total Revenue', f'${total_revenue:,.1f}M', self.colors['success'], '💰'),
            ('Average Rating', f'{avg_rating:.2f}/10', self.colors['warning'], '⭐'),
            ('Average ROI', f'{avg_roi:.1f}%', self.colors['info'], '📈')
        ]
        
        for idx, (title, value, color, emoji) in enumerate(kpi_data):
            ax = fig.add_subplot(gs[0, idx])
            ax.axis('off')
            
            # Create card background
            rect = mpatches.FancyBboxPatch((0.1, 0.1), 0.8, 0.8,
                                          boxstyle="round,pad=0.1",
                                          facecolor=color, alpha=0.2,
                                          edgecolor=color, linewidth=2)
            ax.add_patch(rect)
            
            # Add emoji and title
            ax.text(0.5, 0.7, emoji, ha='center', va='center', 
                   fontsize=40, transform=ax.transAxes)
            ax.text(0.5, 0.45, title, ha='center', va='center',
                   fontsize=12, fontweight='bold', transform=ax.transAxes)
            ax.text(0.5, 0.25, value, ha='center', va='center',
                   fontsize=18, fontweight='bold', color=color,
                   transform=ax.transAxes)
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
        
        # === Revenue vs Budget Scatter (Middle Left) ===
        ax1 = fig.add_subplot(gs[1, :2])
        scatter_data = df.select('budget_musd', 'revenue_musd', 'vote_average') \
                        .filter((F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0)) \
                        .limit(500).toPandas()
        
        scatter = ax1.scatter(scatter_data['budget_musd'], 
                            scatter_data['revenue_musd'],
                            c=scatter_data['vote_average'],
                            cmap='RdYlGn', s=100, alpha=0.6, edgecolors='black')
        
        # Add break-even line
        max_val = max(scatter_data['budget_musd'].max(), scatter_data['revenue_musd'].max())
        ax1.plot([0, max_val], [0, max_val], 'r--', alpha=0.5, linewidth=2, label='Break-even')
        
        ax1.set_xlabel('Budget (Million USD)', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax1.set_title('Revenue vs Budget Analysis', fontsize=14, fontweight='bold')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # Add colorbar
        cbar = plt.colorbar(scatter, ax=ax1)
        cbar.set_label('Rating', fontsize=10)
        
        # === Top 10 Genres by Revenue (Middle Right) ===
        ax2 = fig.add_subplot(gs[1, 2:])
        genre_data = df.withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                      .groupBy('genre') \
                      .agg(F.sum('revenue_musd').alias('total_revenue')) \
                      .orderBy(F.desc('total_revenue')) \
                      .limit(10).toPandas()
        
        bars = ax2.barh(range(len(genre_data)), genre_data['total_revenue'],
                       color=sns.color_palette("viridis", len(genre_data)))
        ax2.set_yticks(range(len(genre_data)))
        ax2.set_yticklabels(genre_data['genre'])
        ax2.set_xlabel('Total Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax2.set_title('Top 10 Genres by Revenue', fontsize=14, fontweight='bold')
        ax2.grid(True, axis='x', alpha=0.3)
        
        # Add value labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax2.text(width, bar.get_y() + bar.get_height()/2,
                    f'${width:,.0f}M',
                    ha='left', va='center', fontsize=9, fontweight='bold')
        
        # === Rating Distribution (Bottom Left) ===
        ax3 = fig.add_subplot(gs[2, 0])
        ratings = df.select('vote_average').filter(F.col('vote_average').isNotNull()).toPandas()
        
        ax3.hist(ratings['vote_average'], bins=20, color=self.colors['info'],
                alpha=0.7, edgecolor='black')
        ax3.axvline(avg_rating, color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {avg_rating:.2f}')
        ax3.set_xlabel('Rating', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Number of Movies', fontsize=12, fontweight='bold')
        ax3.set_title('Rating Distribution', fontsize=14, fontweight='bold')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # === ROI Distribution (Bottom Middle) ===
        ax4 = fig.add_subplot(gs[2, 1])
        roi_data = roi_df.select(
            ((F.col('revenue_musd') / F.col('budget_musd') - 1) * 100).alias('roi')
        ).filter(F.col('roi').between(-100, 500)).toPandas()
        
        ax4.hist(roi_data['roi'], bins=30, color=self.colors['success'],
                alpha=0.7, edgecolor='black')
        ax4.axvline(0, color='red', linestyle='--', linewidth=2, label='Break-even')
        ax4.axvline(avg_roi, color='orange', linestyle='--', linewidth=2,
                   label=f'Mean: {avg_roi:.1f}%')
        ax4.set_xlabel('ROI (%)', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Number of Movies', fontsize=12, fontweight='bold')
        ax4.set_title('ROI Distribution', fontsize=14, fontweight='bold')
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        # === Yearly Revenue Trend (Bottom Right - Spans 2 columns) ===
        ax5 = fig.add_subplot(gs[2, 2:])
        yearly_data = df.withColumn('year', F.year('release_date')) \
                       .groupBy('year') \
                       .agg(F.sum('revenue_musd').alias('total_revenue'),
                           F.count('*').alias('movie_count')) \
                       .orderBy('year').toPandas()
        
        ax5_twin = ax5.twinx()
        
        line1 = ax5.plot(yearly_data['year'], yearly_data['total_revenue'],
                        marker='o', linewidth=2, color=self.colors['primary'],
                        label='Revenue')
        ax5.fill_between(yearly_data['year'], yearly_data['total_revenue'],
                        alpha=0.3, color=self.colors['primary'])
        
        line2 = ax5_twin.plot(yearly_data['year'], yearly_data['movie_count'],
                             marker='s', linewidth=2, color=self.colors['danger'],
                             linestyle='--', label='Movie Count')
        
        ax5.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax5.set_ylabel('Total Revenue (Million USD)', fontsize=12, fontweight='bold',
                      color=self.colors['primary'])
        ax5_twin.set_ylabel('Number of Movies', fontsize=12, fontweight='bold',
                           color=self.colors['danger'])
        ax5.set_title('Revenue Trend Over Time', fontsize=14, fontweight='bold')
        
        # Combine legends
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax5.legend(lines, labels, loc='upper left')
        
        ax5.grid(True, alpha=0.3)
        ax5.tick_params(axis='y', labelcolor=self.colors['primary'])
        ax5_twin.tick_params(axis='y', labelcolor=self.colors['danger'])
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        logger.info(f"Executive summary saved to {output_path}")
        return str(output_path)
    
    def create_financial_performance_dashboard(
        self,
        df: DataFrame,
        output_file: str = "financial_performance.png"
    ) -> str:
        """Create detailed financial performance dashboard"""
        logger.info("Creating financial performance dashboard")
        
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Financial Performance Analysis', fontsize=20, fontweight='bold')
        
        # 1. Budget Distribution by Tier
        budget_tiers = df.withColumn(
            'budget_tier',
            F.when(F.col('budget_musd') < 10, 'Low (<$10M)')
             .when(F.col('budget_musd') < 50, 'Medium ($10M-$50M)')
             .when(F.col('budget_musd') < 100, 'High ($50M-$100M)')
             .otherwise('Blockbuster (>$100M)')
        ).groupBy('budget_tier').count().toPandas()
        
        axes[0, 0].pie(budget_tiers['count'], labels=budget_tiers['budget_tier'],
                      autopct='%1.1f%%', startangle=90,
                      colors=sns.color_palette("Set2"))
        axes[0, 0].set_title('Budget Distribution', fontsize=14, fontweight='bold')
        
        # 2. Top 10 Highest Revenue Movies
        top_revenue = df.select('title', 'revenue_musd') \
                       .orderBy(F.desc('revenue_musd')) \
                       .limit(10).toPandas()
        
        bars = axes[0, 1].barh(range(len(top_revenue)), top_revenue['revenue_musd'],
                              color=self.colors['success'])
        axes[0, 1].set_yticks(range(len(top_revenue)))
        axes[0, 1].set_yticklabels([t[:20] + '...' if len(t) > 20 else t 
                                   for t in top_revenue['title']], fontsize=9)
        axes[0, 1].set_xlabel('Revenue (Million USD)', fontweight='bold')
        axes[0, 1].set_title('Top 10 Highest Revenue', fontsize=14, fontweight='bold')
        axes[0, 1].grid(True, axis='x', alpha=0.3)
        
        # 3. Top 10 Best ROI
        top_roi = df.filter((F.col('budget_musd') >= 10) & (F.col('revenue_musd') > 0)) \
                   .withColumn('roi', (F.col('revenue_musd') / F.col('budget_musd') - 1) * 100) \
                   .select('title', 'roi') \
                   .orderBy(F.desc('roi')) \
                   .limit(10).toPandas()
        
        bars = axes[0, 2].barh(range(len(top_roi)), top_roi['roi'],
                              color=self.colors['warning'])
        axes[0, 2].set_yticks(range(len(top_roi)))
        axes[0, 2].set_yticklabels([t[:20] + '...' if len(t) > 20 else t 
                                   for t in top_roi['title']], fontsize=9)
        axes[0, 2].set_xlabel('ROI (%)', fontweight='bold')
        axes[0, 2].set_title('Top 10 Best ROI', fontsize=14, fontweight='bold')
        axes[0, 2].grid(True, axis='x', alpha=0.3)
        
        # 4. Profit vs Budget Scatter
        profit_data = df.filter((F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0)) \
                       .withColumn('profit', F.col('revenue_musd') - F.col('budget_musd')) \
                       .select('budget_musd', 'profit') \
                       .limit(500).toPandas()
        
        axes[1, 0].scatter(profit_data['budget_musd'], profit_data['profit'],
                          alpha=0.6, s=80, c=self.colors['info'], edgecolors='black')
        axes[1, 0].axhline(0, color='red', linestyle='--', alpha=0.5, label='Break-even')
        axes[1, 0].set_xlabel('Budget (Million USD)', fontweight='bold')
        axes[1, 0].set_ylabel('Profit (Million USD)', fontweight='bold')
        axes[1, 0].set_title('Profit vs Budget', fontsize=14, fontweight='bold')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)
        
        # 5. Revenue by Production Company (Top 10)
        company_revenue = df.withColumn('company', F.explode(F.split('production_companies', '\\|'))) \
                           .groupBy('company') \
                           .agg(F.sum('revenue_musd').alias('total_revenue'),
                               F.count('*').alias('movie_count')) \
                           .filter(F.col('movie_count') >= 3) \
                           .orderBy(F.desc('total_revenue')) \
                           .limit(10).toPandas()
        
        bars = axes[1, 1].barh(range(len(company_revenue)), company_revenue['total_revenue'],
                              color=sns.color_palette("coolwarm", len(company_revenue)))
        axes[1, 1].set_yticks(range(len(company_revenue)))
        axes[1, 1].set_yticklabels([c[:25] + '...' if len(c) > 25 else c 
                                   for c in company_revenue['company']], fontsize=9)
        axes[1, 1].set_xlabel('Total Revenue (Million USD)', fontweight='bold')
        axes[1, 1].set_title('Top Production Companies', fontsize=14, fontweight='bold')
        axes[1, 1].grid(True, axis='x', alpha=0.3)
        
        # 6. Franchise vs Standalone Comparison
        comparison = df.withColumn(
            'type',
            F.when(F.col('belongs_to_collection').isNotNull(), 'Franchise').otherwise('Standalone')
        ).groupBy('type').agg(
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.count('*').alias('count')
        ).toPandas()
        
        x = np.arange(len(comparison))
        width = 0.35
        
        axes[1, 2].bar(x - width/2, comparison['avg_revenue'], width,
                      label='Avg Revenue', color=self.colors['success'])
        axes[1, 2].bar(x + width/2, comparison['avg_budget'], width,
                      label='Avg Budget', color=self.colors['danger'])
        
        axes[1, 2].set_xlabel('Movie Type', fontweight='bold')
        axes[1, 2].set_ylabel('Amount (Million USD)', fontweight='bold')
        axes[1, 2].set_title('Franchise vs Standalone', fontsize=14, fontweight='bold')
        axes[1, 2].set_xticks(x)
        axes[1, 2].set_xticklabels(comparison['type'])
        axes[1, 2].legend()
        axes[1, 2].grid(True, axis='y', alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        logger.info(f"Financial dashboard saved to {output_path}")
        return str(output_path)
    
    def create_quality_metrics_dashboard(
        self,
        df: DataFrame,
        output_file: str = "quality_metrics.png"
    ) -> str:
        """Create quality metrics and rating analysis dashboard"""
        logger.info("Creating quality metrics dashboard")
        
        fig, axes = plt.subplots(2, 3, figsize=(20, 12))
        fig.suptitle('Quality Metrics & Rating Analysis', fontsize=20, fontweight='bold')
        
        # 1. Rating vs Popularity
        rating_pop = df.select('vote_average', 'popularity', 'vote_count') \
                      .filter((F.col('vote_count') >= 100)) \
                      .limit(500).toPandas()
        
        scatter = axes[0, 0].scatter(rating_pop['vote_average'], rating_pop['popularity'],
                                    c=rating_pop['vote_count'], cmap='plasma',
                                    s=100, alpha=0.6, edgecolors='black')
        axes[0, 0].set_xlabel('Vote Average', fontweight='bold')
        axes[0, 0].set_ylabel('Popularity', fontweight='bold')
        axes[0, 0].set_title('Rating vs Popularity', fontsize=14, fontweight='bold')
        axes[0, 0].grid(True, alpha=0.3)
        plt.colorbar(scatter, ax=axes[0, 0], label='Vote Count')
        
        # 2. Top Rated Movies (min 100 votes)
        top_rated = df.filter(F.col('vote_count') >= 100) \
                     .select('title', 'vote_average') \
                     .orderBy(F.desc('vote_average')) \
                     .limit(10).toPandas()
        
        bars = axes[0, 1].barh(range(len(top_rated)), top_rated['vote_average'],
                              color=sns.color_palette("YlGn", len(top_rated))[::-1])
        axes[0, 1].set_yticks(range(len(top_rated)))
        axes[0, 1].set_yticklabels([t[:20] + '...' if len(t) > 20 else t 
                                   for t in top_rated['title']], fontsize=9)
        axes[0, 1].set_xlabel('Rating', fontweight='bold')
        axes[0, 1].set_title('Top 10 Rated Movies', fontsize=14, fontweight='bold')
        axes[0, 1].set_xlim(0, 10)
        axes[0, 1].grid(True, axis='x', alpha=0.3)
        
        # Add rating labels
        for i, bar in enumerate(bars):
            width = bar.get_width()
            axes[0, 1].text(width, bar.get_y() + bar.get_height()/2,
                           f'{width:.2f}',
                           ha='left', va='center', fontsize=9, fontweight='bold')
        
        # 3. Rating by Genre (Violin Plot)
        genre_ratings = df.withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                         .select('genre', 'vote_average') \
                         .filter(F.col('vote_average').isNotNull()).toPandas()
        
        top_genres = genre_ratings['genre'].value_counts().head(8).index.tolist()
        genre_ratings_filtered = genre_ratings[genre_ratings['genre'].isin(top_genres)]
        
        parts = axes[0, 2].violinplot([genre_ratings_filtered[genre_ratings_filtered['genre'] == g]['vote_average'].values
                                       for g in top_genres],
                                      positions=range(len(top_genres)),
                                      showmeans=True, showmedians=True)
        
        for pc in parts['bodies']:
            pc.set_facecolor(self.colors['info'])
            pc.set_alpha(0.7)
        
        axes[0, 2].set_xticks(range(len(top_genres)))
        axes[0, 2].set_xticklabels(top_genres, rotation=45, ha='right')
        axes[0, 2].set_ylabel('Rating', fontweight='bold')
        axes[0, 2].set_title('Rating Distribution by Genre', fontsize=14, fontweight='bold')
        axes[0, 2].grid(True, axis='y', alpha=0.3)
        
        # 4. Vote Count Distribution
        vote_counts = df.select('vote_count').filter(F.col('vote_count') > 0).toPandas()
        
        axes[1, 0].hist(vote_counts['vote_count'], bins=50, color=self.colors['primary'],
                       alpha=0.7, edgecolor='black', log=True)
        axes[1, 0].set_xlabel('Vote Count', fontweight='bold')
        axes[1, 0].set_ylabel('Frequency (log scale)', fontweight='bold')
        axes[1, 0].set_title('Vote Count Distribution', fontsize=14, fontweight='bold')
        axes[1, 0].grid(True, alpha=0.3)
        
        # 5. Rating Trend Over Years
        yearly_ratings = df.withColumn('year', F.year('release_date')) \
                          .groupBy('year') \
                          .agg(F.avg('vote_average').alias('avg_rating'),
                              F.stddev('vote_average').alias('std_rating')) \
                          .orderBy('year').toPandas()
        
        axes[1, 1].plot(yearly_ratings['year'], yearly_ratings['avg_rating'],
                       marker='o', linewidth=2, color=self.colors['success'])
        axes[1, 1].fill_between(yearly_ratings['year'],
                               yearly_ratings['avg_rating'] - yearly_ratings['std_rating'],
                               yearly_ratings['avg_rating'] + yearly_ratings['std_rating'],
                               alpha=0.3, color=self.colors['success'])
        axes[1, 1].set_xlabel('Year', fontweight='bold')
        axes[1, 1].set_ylabel('Average Rating', fontweight='bold')
        axes[1, 1].set_title('Rating Trend Over Time', fontsize=14, fontweight='bold')
        axes[1, 1].grid(True, alpha=0.3)
        
        # 6. Most Popular Movies
        top_popular = df.select('title', 'popularity') \
                       .orderBy(F.desc('popularity')) \
                       .limit(10).toPandas()
        
        bars = axes[1, 2].barh(range(len(top_popular)), top_popular['popularity'],
                              color=sns.color_palette("rocket", len(top_popular)))
        axes[1, 2].set_yticks(range(len(top_popular)))
        axes[1, 2].set_yticklabels([t[:20] + '...' if len(t) > 20 else t 
                                   for t in top_popular['title']], fontsize=9)
        axes[1, 2].set_xlabel('Popularity Score', fontweight='bold')
        axes[1, 2].set_title('Top 10 Most Popular', fontsize=14, fontweight='bold')
        axes[1, 2].grid(True, axis='x', alpha=0.3)
        
        plt.tight_layout()
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        logger.info(f"Quality metrics dashboard saved to {output_path}")
        return str(output_path)

    def create_genre_analysis_dashboard(
        self,
        df: DataFrame,
        output_file: str = "genre_analysis.png"
    ) -> str:
        """Create comprehensive genre analysis dashboard"""
        logger.info("Creating genre analysis dashboard")
        
        fig = plt.figure(figsize=(20, 14))
        gs = fig.add_gridspec(3, 3, hspace=0.35, wspace=0.3)
        
        fig.suptitle('Genre Analysis Deep Dive', fontsize=20, fontweight='bold')
        
        # Explode genres for analysis
        genre_df = df.withColumn('genre', F.explode(F.split('genres', '\\|')))
        
        # 1. Genre Revenue Breakdown (Pie Chart - Top 8)
        ax1 = fig.add_subplot(gs[0, 0])
        genre_revenue = genre_df.groupBy('genre') \
                               .agg(F.sum('revenue_musd').alias('total_revenue')) \
                               .orderBy(F.desc('total_revenue')) \
                               .limit(8).toPandas()
        
        colors_pie = sns.color_palette("Set3", len(genre_revenue))
        wedges, texts, autotexts = ax1.pie(genre_revenue['total_revenue'],
                                           labels=genre_revenue['genre'],
                                           autopct='%1.1f%%',
                                           startangle=90,
                                           colors=colors_pie,
                                           explode=[0.05] * len(genre_revenue))
        
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        ax1.set_title('Revenue Distribution by Genre (Top 8)', 
                     fontsize=14, fontweight='bold', pad=20)
        
        # 2. Genre Movie Count
        ax2 = fig.add_subplot(gs[0, 1:])
        genre_count = genre_df.groupBy('genre').count() \
                             .orderBy(F.desc('count')) \
                             .limit(15).toPandas()
        
        bars = ax2.bar(range(len(genre_count)), genre_count['count'],
                      color=sns.color_palette("viridis", len(genre_count)))
        ax2.set_xticks(range(len(genre_count)))
        ax2.set_xticklabels(genre_count['genre'], rotation=45, ha='right')
        ax2.set_ylabel('Number of Movies', fontsize=12, fontweight='bold')
        ax2.set_title('Movie Count by Genre', fontsize=14, fontweight='bold')
        ax2.grid(True, axis='y', alpha=0.3)
        
        # Add value labels on bars
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2, height,
                    f'{int(height)}',
                    ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        # 3. Genre Performance Metrics (Heatmap)
        ax3 = fig.add_subplot(gs[1, :])
        genre_metrics = genre_df.groupBy('genre').agg(
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('budget_musd').alias('avg_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.avg('popularity').alias('avg_popularity'),
            F.count('*').alias('count')
        ).filter(F.col('count') >= 5) \
         .orderBy(F.desc('avg_revenue')) \
         .limit(12).toPandas()
        
        # Normalize metrics for heatmap
        metrics_for_heatmap = genre_metrics[['avg_revenue', 'avg_budget', 
                                             'avg_rating', 'avg_popularity']].copy()
        metrics_normalized = (metrics_for_heatmap - metrics_for_heatmap.min()) / \
                           (metrics_for_heatmap.max() - metrics_for_heatmap.min())
        
        im = ax3.imshow(metrics_normalized.T, cmap='YlOrRd', aspect='auto')
        
        ax3.set_xticks(range(len(genre_metrics)))
        ax3.set_xticklabels(genre_metrics['genre'], rotation=45, ha='right')
        ax3.set_yticks(range(len(metrics_normalized.columns)))
        ax3.set_yticklabels(['Avg Revenue', 'Avg Budget', 'Avg Rating', 'Avg Popularity'])
        ax3.set_title('Genre Performance Heatmap (Normalized)', 
                     fontsize=14, fontweight='bold')
        
        # Add colorbar
        cbar = plt.colorbar(im, ax=ax3)
        cbar.set_label('Normalized Score', rotation=270, labelpad=20)
        
        # Add text annotations
        for i in range(len(metrics_normalized.columns)):
            for j in range(len(genre_metrics)):
                text = ax3.text(j, i, f'{metrics_normalized.iloc[j, i]:.2f}',
                              ha="center", va="center", color="black", fontsize=8)
        
        # 4. Genre ROI Comparison
        ax4 = fig.add_subplot(gs[2, 0])
        genre_roi = genre_df.filter((F.col('budget_musd') > 10) & 
                                   (F.col('revenue_musd') > 0)) \
                           .withColumn('roi', (F.col('revenue_musd') / 
                                             F.col('budget_musd') - 1) * 100) \
                           .groupBy('genre') \
                           .agg(F.avg('roi').alias('avg_roi'),
                               F.count('*').alias('count')) \
                           .filter(F.col('count') >= 5) \
                           .orderBy(F.desc('avg_roi')) \
                           .limit(10).toPandas()
        
        colors_roi = ['green' if x > 100 else 'orange' if x > 0 else 'red' 
                     for x in genre_roi['avg_roi']]
        
        bars = ax4.barh(range(len(genre_roi)), genre_roi['avg_roi'], color=colors_roi)
        ax4.set_yticks(range(len(genre_roi)))
        ax4.set_yticklabels(genre_roi['genre'])
        ax4.set_xlabel('Average ROI (%)', fontsize=12, fontweight='bold')
        ax4.set_title('Best ROI by Genre', fontsize=14, fontweight='bold')
        ax4.axvline(0, color='black', linestyle='--', alpha=0.5)
        ax4.grid(True, axis='x', alpha=0.3)
        
        # 5. Genre Rating vs Revenue Scatter
        ax5 = fig.add_subplot(gs[2, 1])
        genre_scatter = genre_df.groupBy('genre').agg(
            F.avg('revenue_musd').alias('avg_revenue'),
            F.avg('vote_average').alias('avg_rating'),
            F.count('*').alias('count')
        ).filter(F.col('count') >= 5).toPandas()
        
        scatter = ax5.scatter(genre_scatter['avg_rating'],
                            genre_scatter['avg_revenue'],
                            s=genre_scatter['count']*20,
                            alpha=0.6,
                            c=range(len(genre_scatter)),
                            cmap='rainbow',
                            edgecolors='black')
        
        # Add genre labels
        for idx, row in genre_scatter.iterrows():
            ax5.annotate(row['genre'], 
                        (row['avg_rating'], row['avg_revenue']),
                        fontsize=8, alpha=0.7)
        
        ax5.set_xlabel('Average Rating', fontsize=12, fontweight='bold')
        ax5.set_ylabel('Average Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax5.set_title('Genre Quality vs Revenue', fontsize=14, fontweight='bold')
        ax5.grid(True, alpha=0.3)
        
        # 6. Genre Evolution (Timeline)
        ax6 = fig.add_subplot(gs[2, 2])
        
        # Get top 5 genres and their yearly revenue
        top_genres_list = genre_revenue.head(5)['genre'].tolist()
        
        for genre in top_genres_list:
            genre_yearly = genre_df.filter(F.col('genre') == genre) \
                                  .withColumn('year', F.year('release_date')) \
                                  .groupBy('year') \
                                  .agg(F.sum('revenue_musd').alias('revenue')) \
                                  .orderBy('year').toPandas()
            
            ax6.plot(genre_yearly['year'], genre_yearly['revenue'],
                    marker='o', label=genre, linewidth=2)
        
        ax6.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax6.set_ylabel('Revenue (Million USD)', fontsize=12, fontweight='bold')
        ax6.set_title('Genre Revenue Evolution', fontsize=14, fontweight='bold')
        ax6.legend(loc='best', fontsize=9)
        ax6.grid(True, alpha=0.3)
        
        output_path = self.viz_dir / output_file
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        plt.close()
        
        logger.info(f"Genre analysis dashboard saved to {output_path}")
        return str(output_path)
    
    def create_interactive_html_dashboard(
        self,
        df: DataFrame,
        kpis: Dict[str, Any],
        output_file: str = "interactive_dashboard.html"
    ) -> str:
        """Create interactive HTML dashboard with all insights"""
        logger.info("Creating interactive HTML dashboard")
        
        # Calculate key metrics
        stats = df.agg(
            F.count('*').alias('total_movies'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.avg('revenue_musd').alias('avg_revenue'),
            F.sum('budget_musd').alias('total_budget'),
            F.avg('vote_average').alias('avg_rating'),
            F.countDistinct('original_language').alias('languages'),
            F.countDistinct('director').alias('directors')
        ).collect()[0]
        
        # Get top performers
        top_revenue_list = df.select('title', 'revenue_musd', 'vote_average') \
                            .orderBy(F.desc('revenue_musd')) \
                            .limit(10).toPandas()
        
        top_rated_list = df.filter(F.col('vote_count') >= 100) \
                          .select('title', 'vote_average', 'vote_count') \
                          .orderBy(F.desc('vote_average')) \
                          .limit(10).toPandas()
        
        # Genre data
        genre_stats = df.withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                       .groupBy('genre') \
                       .agg(F.count('*').alias('count'),
                           F.sum('revenue_musd').alias('total_revenue')) \
                       .orderBy(F.desc('total_revenue')) \
                       .limit(10).toPandas()
        
        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TMDB Movie Analysis Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #333;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}
        
        header {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
            text-align: center;
        }}
        
        h1 {{
            color: #667eea;
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .subtitle {{
            color: #666;
            font-size: 1.1em;
        }}
        
        .timestamp {{
            color: #999;
            font-size: 0.9em;
            margin-top: 10px;
        }}
        
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .metric-card {{
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s, box-shadow 0.3s;
        }}
        
        .metric-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(0,0,0,0.2);
        }}
        
        .metric-icon {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .metric-value {{
            font-size: 2em;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }}
        
        .metric-label {{
            color: #666;
            font-size: 0.95em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .section {{
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        
        h2 {{
            color: #667eea;
            font-size: 1.8em;
            margin-bottom: 20px;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        
        th, td {{
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.9em;
            letter-spacing: 1px;
        }}
        
        tr:hover {{
            background: #f8f9ff;
        }}
        
        .chart-container {{
            margin: 20px 0;
            padding: 20px;
            background: #f8f9ff;
            border-radius: 10px;
        }}
        
        .badge {{
            display: inline-block;
            padding: 5px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            margin: 0 5px;
        }}
        
        .badge-success {{
            background: #2ecc71;
            color: white;
        }}
        
        .badge-warning {{
            background: #f39c12;
            color: white;
        }}
        
        .badge-info {{
            background: #3498db;
            color: white;
        }}
        
        .grid-2 {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
        }}
        
        @media (max-width: 768px) {{
            .grid-2 {{
                grid-template-columns: 1fr;
            }}
            
            .metrics-grid {{
                grid-template-columns: 1fr;
            }}
        }}
        
        .progress-bar {{
            background: #eee;
            height: 8px;
            border-radius: 4px;
            overflow: hidden;
            margin: 10px 0;
        }}
        
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s ease;
        }}
        
        footer {{
            text-align: center;
            color: white;
            margin-top: 40px;
            padding: 20px;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🎬 TMDB Movie Data Analysis Dashboard</h1>
            <p class="subtitle">Comprehensive Insights from The Movie Database</p>
            <p class="timestamp">Generated: {datetime.now().strftime("%B %d, %Y at %I:%M %p")}</p>
        </header>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-icon">🎬</div>
                <div class="metric-value">{stats['total_movies']:,}</div>
                <div class="metric-label">Total Movies</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">💰</div>
                <div class="metric-value">${stats['total_revenue']:,.0f}M</div>
                <div class="metric-label">Total Revenue</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">⭐</div>
                <div class="metric-value">{stats['avg_rating']:.2f}/10</div>
                <div class="metric-label">Average Rating</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">🎭</div>
                <div class="metric-value">{stats['directors']:,}</div>
                <div class="metric-label">Unique Directors</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">🌍</div>
                <div class="metric-value">{stats['languages']:,}</div>
                <div class="metric-label">Languages</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-icon">💵</div>
                <div class="metric-value">${stats['avg_revenue']:,.0f}M</div>
                <div class="metric-label">Avg Revenue</div>
            </div>
        </div>
        
        <div class="grid-2">
            <div class="section">
                <h2>📊 Top 10 Highest Revenue Movies</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Title</th>
                            <th>Revenue</th>
                            <th>Rating</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for idx, row in top_revenue_list.iterrows():
            rating_badge = 'success' if row['vote_average'] >= 7 else 'warning'
            html_content += f"""
                        <tr>
                            <td><strong>{idx + 1}</strong></td>
                            <td>{row['title']}</td>
                            <td><strong>${row['revenue_musd']:,.0f}M</strong></td>
                            <td><span class="badge badge-{rating_badge}">{row['vote_average']:.1f}/10</span></td>
                        </tr>
            """
        
        html_content += """
                    </tbody>
                </table>
            </div>
            
            <div class="section">
                <h2>⭐ Top 10 Highest Rated Movies</h2>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>Title</th>
                            <th>Rating</th>
                            <th>Votes</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        
        for idx, row in top_rated_list.iterrows():
            html_content += f"""
                        <tr>
                            <td><strong>{idx + 1}</strong></td>
                            <td>{row['title']}</td>
                            <td><span class="badge badge-success">{row['vote_average']:.2f}/10</span></td>
                            <td>{row['vote_count']:,} votes</td>
                        </tr>
            """
        
        html_content += """
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="section">
            <h2>🎭 Genre Performance Analysis</h2>
            <div class="chart-container">
        """
        
        max_revenue = genre_stats['total_revenue'].max()
        for _, row in genre_stats.iterrows():
            percentage = (row['total_revenue'] / max_revenue) * 100
            html_content += f"""
                <div style="margin-bottom: 20px;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 5px;">
                        <span><strong>{row['genre']}</strong></span>
                        <span>${row['total_revenue']:,.0f}M ({row['count']} movies)</span>
                    </div>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: {percentage}%;"></div>
                    </div>
                </div>
            """
        
        html_content += f"""
            </div>
        </div>
        
        <div class="section">
            <h2>📈 Key Insights</h2>
            <div style="line-height: 2;">
                <p>✅ <strong>Total Revenue:</strong> The analyzed movies generated ${stats['total_revenue']:,.0f}M in total revenue</p>
                <p>✅ <strong>Average Budget:</strong> Movies had an average budget of ${stats['total_budget']/stats['total_movies']:,.0f}M</p>
                <p>✅ <strong>Quality Score:</strong> Average rating of {stats['avg_rating']:.2f}/10 indicates strong overall quality</p>
                <p>✅ <strong>Diversity:</strong> Content spans {stats['languages']} languages and {stats['directors']} directors</p>
                <p>✅ <strong>Top Genre:</strong> {genre_stats.iloc[0]['genre']} leads with ${genre_stats.iloc[0]['total_revenue']:,.0f}M in revenue</p>
            </div>
        </div>
        
        <footer>
            <p>📊 TMDB Movie Data Analysis Pipeline | Built with PySpark & Python</p>
            <p>Data Source: The Movie Database (TMDB) API</p>
        </footer>
    </div>
</body>
</html>
        """
        
        output_path = self.viz_dir / output_file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        logger.info(f"Interactive HTML dashboard saved to {output_path}")
        return str(output_path)
    
    def generate_all_dashboards(
        self,
        df: DataFrame,
        kpis: Dict[str, Any]
    ) -> Dict[str, str]:
        """Generate all visualization dashboards"""
        logger.info("Generating all visualization dashboards")
        
        dashboards = {}
        
        try:
            dashboards['executive_summary'] = self.create_executive_summary_dashboard(df, kpis)
            dashboards['financial_performance'] = self.create_financial_performance_dashboard(df)
            dashboards['quality_metrics'] = self.create_quality_metrics_dashboard(df)
            dashboards['genre_analysis'] = self.create_genre_analysis_dashboard(df)
            dashboards['interactive_html'] = self.create_interactive_html_dashboard(df, kpis)

            # Generate Plotly interactive dashboards if available
            if _HAS_PLOTLY and PlotlyDashboardGenerator is not None:
                try:
                    pgen = PlotlyDashboardGenerator(self.config)
                    visual_interactive = pgen.generate_all_interactive_visualizations(df, kpis)
                    dashboards['interactive_plotly'] = visual_interactive
                except Exception as e:
                    logger.warning(f"Plotly visualizations failed: {e}")
            else:
                logger.info("Plotly not available — skipping interactive Plotly dashboards")

            logger.info(f"Generated {len(dashboards)} dashboards successfully")
            
        except Exception as e:
            logger.error(f"Error generating dashboards: {str(e)}")
            raise
        
        return dashboards

    def generate_all_visualizations(
        self,
        df: DataFrame,
        kpis: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """Backward-compatible alias for older callers.

        Older code (DAGs/tasks) may call `generate_all_visualizations` —
        delegate to the current `generate_all_dashboards` implementation.
        """
        logger.info("generate_all_visualizations() called — delegating to generate_all_dashboards")
        return self.generate_all_dashboards(df, kpis or {})


if __name__ == '__main__':
    import argparse
    import json
    import logging
    from pathlib import Path
    from src.config import load_config
    from src.utils.spark_session import create_optimized_session

    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger('dashboard_generator_cli')

    parser = argparse.ArgumentParser(description='Run DashboardGenerator from CLI')
    parser.add_argument('--config', '-c', help='Path to config.yaml', default=None)
    parser.add_argument('--input', '-i', help='Parquet input path (overrides auto-discovery)', default=None)
    parser.add_argument('--env', help='Environment for Spark (development/testing/production)', default='development')
    args = parser.parse_args()

    # Load config
    try:
        cfg = load_config(args.config) if args.config else load_config()
    except Exception as e:
        log.error(f'Failed to load config: {e}')
        raise

    # Determine input parquet path
    input_path = args.input
    if not input_path:
        # Resolve project root (two levels up from `src/visualization`)
        project_root = Path(__file__).resolve().parents[2]

        # Try to read output base from config to infer container mounts
        cfg_output_base = None
        try:
            cfg_output_base = Path(cfg.get('output', {}).get('base_path', '')) if cfg else None
        except Exception:
            cfg_output_base = None

        # Candidate locations to look for processed runs (works locally and in-container)
        candidate_dirs = [
            project_root / 'data' / 'processed',   # local dev layout
            Path.cwd() / 'data' / 'processed',     # invoked from repo root
        ]

        if cfg_output_base and cfg_output_base.exists():
            candidate_dirs.append(cfg_output_base.parent / 'processed')

        # Common container mount used by docker-compose
        candidate_dirs.append(Path('/opt/spark-data') / 'processed')

        # Find the first candidate that exists and has transformed runs
        latest = None
        tried = []
        for cand in candidate_dirs:
            tried.append(str(cand))
            if cand.exists():
                runs = sorted([p for p in cand.iterdir() if p.is_dir() and p.name.startswith('movies_transformed_run_')])
                if runs:
                    latest = runs[-1]
                    input_path = str(latest)
                    log.info(f'Auto-selected latest transformed folder: {input_path}')
                    break

        if not latest:
            log.error('Processed data directory not found. Tried: %s', tried)
            raise SystemExit(1)

    # Start Spark
    spark = create_optimized_session('dashboard-generator', environment=args.env)

    # Read parquet
    log.info(f'Reading parquet from {input_path}')
    df = spark.read.parquet(input_path)

    # Generate dashboards
    gen = DashboardGenerator(cfg)
    kpis = cfg.get('kpis', {})

    log.info('Generating dashboards...')
    dashboards = gen.generate_all_dashboards(df, kpis)

    # Print output paths as JSON
    print(json.dumps(dashboards, indent=2))