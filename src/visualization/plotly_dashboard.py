"""
Plotly Interactive Dashboard Generator
"""

import logging
from typing import Dict, Any, List
from pathlib import Path
import json

import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class PlotlyDashboardGenerator:
    """Generate interactive Plotly visualizations"""
    
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
        self.viz_dir = self.output_dir / 'visualizations' / 'interactive'
        self.viz_dir.mkdir(parents=True, exist_ok=True)
        
        # Color theme
        self.colors = px.colors.qualitative.Plotly
    
    def create_revenue_budget_scatter(
        self,
        df: DataFrame,
        output_file: str = "revenue_budget_interactive.html"
    ) -> str:
        """Create interactive revenue vs budget scatter plot"""
        logger.info("Creating interactive revenue vs budget scatter")
        
        plot_data = df.select(
            'title', 'budget_musd', 'revenue_musd', 
            'vote_average', 'genres', 'release_date'
        ).filter(
            (F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0)
        ).limit(1000).toPandas()
        
        # Calculate profit
        plot_data['profit_musd'] = plot_data['revenue_musd'] - plot_data['budget_musd']
        plot_data['roi_percent'] = ((plot_data['revenue_musd'] / plot_data['budget_musd']) - 1) * 100
        
        fig = px.scatter(
            plot_data,
            x='budget_musd',
            y='revenue_musd',
            size='profit_musd',
            color='vote_average',
            hover_data=['title', 'genres', 'roi_percent'],
            title='<b>Movie Revenue vs Budget Analysis</b>',
            labels={
                'budget_musd': 'Budget (Million USD)',
                'revenue_musd': 'Revenue (Million USD)',
                'vote_average': 'Rating',
                'profit_musd': 'Profit'
            },
            color_continuous_scale='RdYlGn',
            size_max=50
        )
        
        # Add break-even line
        max_val = max(plot_data['budget_musd'].max(), plot_data['revenue_musd'].max())
        fig.add_trace(go.Scatter(
            x=[0, max_val],
            y=[0, max_val],
            mode='lines',
            name='Break-even Line',
            line=dict(color='red', dash='dash', width=2)
        ))
        
        fig.update_layout(
            template='plotly_white',
            height=700,
            hovermode='closest',
            font=dict(size=12),
            title_font_size=18
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"Interactive scatter plot saved to {output_path}")
        return str(output_path)
    
    def create_genre_sunburst(
        self,
        df: DataFrame,
        output_file: str = "genre_sunburst.html"
    ) -> str:
        """Create sunburst chart for genre hierarchy and revenue"""
        logger.info("Creating genre sunburst chart")
        
        # Prepare data
        genre_data = df.withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                      .groupBy('genre') \
                      .agg(
                          F.sum('revenue_musd').alias('total_revenue'),
                          F.count('*').alias('count')
                      ).toPandas()
        
        # Create sunburst
        fig = px.sunburst(
            genre_data,
            names='genre',
            values='total_revenue',
            title='<b>Genre Revenue Distribution</b>',
            color='total_revenue',
            color_continuous_scale='Viridis',
            hover_data=['count']
        )
        
        fig.update_layout(
            template='plotly_white',
            height=700,
            font=dict(size=12)
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"Genre sunburst saved to {output_path}")
        return str(output_path)
    
    def create_timeline_animation(
        self,
        df: DataFrame,
        output_file: str = "revenue_timeline.html"
    ) -> str:
        """Create animated timeline of revenue trends"""
        logger.info("Creating revenue timeline animation")
        
        timeline_data = df.withColumn('year', F.year('release_date')) \
                         .filter(F.col('year').isNotNull()) \
                         .groupBy('year') \
                         .agg(
                             F.sum('revenue_musd').alias('total_revenue'),
                             F.avg('revenue_musd').alias('avg_revenue'),
                             F.count('*').alias('movie_count')
                         ).orderBy('year').toPandas()
        
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Total Revenue by Year', 'Number of Movies Released'),
            vertical_spacing=0.15
        )
        
        # Revenue line chart
        fig.add_trace(
            go.Scatter(
                x=timeline_data['year'],
                y=timeline_data['total_revenue'],
                mode='lines+markers',
                name='Total Revenue',
                line=dict(color='#3498db', width=3),
                fill='tozeroy',
                fillcolor='rgba(52, 152, 219, 0.2)',
                hovertemplate='<b>Year:</b> %{x}<br><b>Revenue:</b> $%{y:,.0f}M<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Movie count bar chart
        fig.add_trace(
            go.Bar(
                x=timeline_data['year'],
                y=timeline_data['movie_count'],
                name='Movie Count',
                marker_color='#2ecc71',
                hovertemplate='<b>Year:</b> %{x}<br><b>Movies:</b> %{y}<extra></extra>'
            ),
            row=2, col=1
        )
        
        fig.update_xaxes(title_text="Year", row=2, col=1)
        fig.update_yaxes(title_text="Revenue (Million USD)", row=1, col=1)
        fig.update_yaxes(title_text="Number of Movies", row=2, col=1)
        
        fig.update_layout(
            template='plotly_white',
            title='<b>Movie Industry Timeline Analysis</b>',
            height=800,
            showlegend=True,
            hovermode='x unified'
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"Timeline animation saved to {output_path}")
        return str(output_path)
    
    def create_roi_box_plot(
        self,
        df: DataFrame,
        output_file: str = "roi_box_plot.html"
    ) -> str:
        """Create box plot for ROI distribution by genre"""
        logger.info("Creating ROI box plot")
        
        roi_data = df.filter((F.col('budget_musd') >= 10) & (F.col('revenue_musd') > 0)) \
                    .withColumn('roi', (F.col('revenue_musd') / F.col('budget_musd') - 1) * 100) \
                    .withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                    .select('genre', 'roi', 'title') \
                    .filter(F.col('roi').between(-100, 500)).toPandas()
        
        # Get top 12 genres by count
        top_genres = roi_data['genre'].value_counts().head(12).index.tolist()
        roi_data_filtered = roi_data[roi_data['genre'].isin(top_genres)]
        
        fig = px.box(
            roi_data_filtered,
            x='genre',
            y='roi',
            color='genre',
            title='<b>ROI Distribution by Genre</b>',
            labels={'roi': 'ROI (%)', 'genre': 'Genre'},
            hover_data=['title']
        )
        
        # Add break-even line
        fig.add_hline(
            y=0,
            line_dash="dash",
            line_color="red",
            annotation_text="Break-even",
            annotation_position="right"
        )
        
        fig.update_layout(
            template='plotly_white',
            height=700,
            showlegend=False,
            xaxis_tickangle=-45
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"ROI box plot saved to {output_path}")
        return str(output_path)
    
    def create_3d_performance_scatter(
        self,
        df: DataFrame,
        output_file: str = "3d_performance.html"
    ) -> str:
        """Create 3D scatter plot of budget, revenue, and rating"""
        logger.info("Creating 3D performance scatter")
        
        plot_data = df.select(
            'title', 'budget_musd', 'revenue_musd', 'vote_average',
            'popularity', 'genres'
        ).filter(
            (F.col('budget_musd') > 0) & 
            (F.col('revenue_musd') > 0) &
            (F.col('vote_average') > 0)
        ).limit(500).toPandas()
        
        fig = px.scatter_3d(
            plot_data,
            x='budget_musd',
            y='revenue_musd',
            z='vote_average',
            size='popularity',
            color='vote_average',
            hover_data=['title', 'genres'],
            title='<b>3D Performance Analysis: Budget vs Revenue vs Rating</b>',
            labels={
                'budget_musd': 'Budget (Million USD)',
                'revenue_musd': 'Revenue (Million USD)',
                'vote_average': 'Rating',
                'popularity': 'Popularity'
            },
            color_continuous_scale='Turbo',
            size_max=30
        )
        
        fig.update_layout(
            template='plotly_white',
            height=800,
            scene=dict(
                xaxis_title='Budget ($M)',
                yaxis_title='Revenue ($M)',
                zaxis_title='Rating',
                camera=dict(
                    eye=dict(x=1.5, y=1.5, z=1.3)
                )
            )
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"3D scatter plot saved to {output_path}")
        return str(output_path)
    
    def create_top_performers_treemap(
        self,
        df: DataFrame,
        output_file: str = "top_performers_treemap.html"
    ) -> str:
        """Create treemap of top performing movies"""
        logger.info("Creating top performers treemap")
        
        # Get top movies by revenue
        top_movies = df.select('title', 'revenue_musd', 'genres', 'vote_average') \
                      .orderBy(F.desc('revenue_musd')) \
                      .limit(50).toPandas()
        
        # Extract primary genre
        top_movies['primary_genre'] = top_movies['genres'].str.split('|').str[0]
        
        fig = px.treemap(
            top_movies,
            path=['primary_genre', 'title'],
            values='revenue_musd',
            color='vote_average',
            color_continuous_scale='RdYlGn',
            title='<b>Top 50 Movies by Revenue</b>',
            hover_data=['vote_average'],
            color_continuous_midpoint=7
        )
        
        fig.update_layout(
            template='plotly_white',
            height=700
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"Treemap saved to {output_path}")
        return str(output_path)
    
    def create_rating_heatmap(
        self,
        df: DataFrame,
        output_file: str = "rating_heatmap.html"
    ) -> str:
        """Create heatmap of ratings by year and genre"""
        logger.info("Creating rating heatmap")
        
        heatmap_data = df.withColumn('year', F.year('release_date')) \
                        .withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                        .groupBy('year', 'genre') \
                        .agg(F.avg('vote_average').alias('avg_rating')) \
                        .toPandas()
        
        # Pivot for heatmap
        pivot_data = heatmap_data.pivot(
            index='genre',
            columns='year',
            values='avg_rating'
        )
        
        fig = px.imshow(
            pivot_data,
            title='<b>Average Rating Heatmap: Genre vs Year</b>',
            labels=dict(x="Year", y="Genre", color="Avg Rating"),
            color_continuous_scale='RdYlGn',
            aspect='auto'
        )
        
        fig.update_layout(
            template='plotly_white',
            height=700,
            xaxis_tickangle=-45
        )
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"Rating heatmap saved to {output_path}")
        return str(output_path)
    
    def create_comprehensive_dashboard(
        self,
        df: DataFrame,
        kpis: Dict[str, Any],
        output_file: str = "comprehensive_dashboard.html"
    ) -> str:
        """Create comprehensive multi-chart dashboard"""
        logger.info("Creating comprehensive Plotly dashboard")
        
        # Calculate metrics
        total_movies = df.count()
        total_revenue = df.select(F.sum('revenue_musd')).collect()[0][0] or 0
        avg_rating = df.select(F.avg('vote_average')).collect()[0][0] or 0
        
        # Create subplots
        fig = make_subplots(
            rows=3, cols=2,
            subplot_titles=(
                'Revenue Distribution',
                'Top 10 Genres by Count',
                'Budget vs Revenue',
                'Rating Distribution',
                'Yearly Revenue Trend',
                'ROI by Budget Tier'
            ),
            specs=[
                [{"type": "histogram"}, {"type": "bar"}],
                [{"type": "scatter"}, {"type": "violin"}],
                [{"type": "scatter"}, {"type": "box"}]
            ],
            vertical_spacing=0.12,
            horizontal_spacing=0.1
        )
        
        # 1. Revenue Distribution
        revenue_data = df.select('revenue_musd').filter(F.col('revenue_musd') > 0).toPandas()
        fig.add_trace(
            go.Histogram(
                x=revenue_data['revenue_musd'],
                nbinsx=50,
                name='Revenue',
                marker_color='#3498db'
            ),
            row=1, col=1
        )
        
        # 2. Top Genres
        genre_count = df.withColumn('genre', F.explode(F.split('genres', '\\|'))) \
                       .groupBy('genre').count() \
                       .orderBy(F.desc('count')) \
                       .limit(10).toPandas()
        
        fig.add_trace(
            go.Bar(
                x=genre_count['genre'],
                y=genre_count['count'],
                name='Count',
                marker_color='#2ecc71'
            ),
            row=1, col=2
        )
        
        # 3. Budget vs Revenue
        scatter_data = df.select('budget_musd', 'revenue_musd') \
                        .filter((F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0)) \
                        .limit(500).toPandas()
        
        fig.add_trace(
            go.Scatter(
                x=scatter_data['budget_musd'],
                y=scatter_data['revenue_musd'],
                mode='markers',
                name='Movies',
                marker=dict(color='#e74c3c', opacity=0.6)
            ),
            row=2, col=1
        )
        
        # 4. Rating Distribution
        rating_data = df.select('vote_average').filter(F.col('vote_average').isNotNull()).toPandas()
        fig.add_trace(
            go.Violin(
                y=rating_data['vote_average'],
                name='Ratings',
                box_visible=True,
                meanline_visible=True,
                fillcolor='#9b59b6',
                opacity=0.6
            ),
            row=2, col=2
        )
        
        # 5. Yearly Trend
        yearly = df.withColumn('year', F.year('release_date')) \
                  .groupBy('year') \
                  .agg(F.sum('revenue_musd').alias('revenue')) \
                  .orderBy('year').toPandas()
        
        fig.add_trace(
            go.Scatter(
                x=yearly['year'],
                y=yearly['revenue'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#f39c12', width=2),
                fill='tozeroy'
            ),
            row=3, col=1
        )
        
        # 6. ROI by Budget Tier
        roi_data = df.filter((F.col('budget_musd') > 0) & (F.col('revenue_musd') > 0)) \
                    .withColumn('roi', (F.col('revenue_musd') / F.col('budget_musd') - 1) * 100) \
                    .withColumn(
                        'budget_tier',
                        F.when(F.col('budget_musd') < 10, 'Low')
                         .when(F.col('budget_musd') < 50, 'Medium')
                         .when(F.col('budget_musd') < 100, 'High')
                         .otherwise('Blockbuster')
                    ).select('budget_tier', 'roi') \
                    .filter(F.col('roi').between(-100, 500)).toPandas()
        
        for tier in ['Low', 'Medium', 'High', 'Blockbuster']:
            tier_data = roi_data[roi_data['budget_tier'] == tier]
            fig.add_trace(
                go.Box(
                    y=tier_data['roi'],
                    name=tier,
                    boxmean='sd'
                ),
                row=3, col=2
            )
        
        # Update layout
        fig.update_layout(
            title_text=f"<b>TMDB Movie Analysis Dashboard</b><br><sub>Total Movies: {total_movies:,} | " \
                      f"Total Revenue: ${total_revenue:,.0f}M | Avg Rating: {avg_rating:.2f}/10</sub>",
            showlegend=False,
            template='plotly_white',
            height=1200,
            font=dict(size=10)
        )
        
        # Update axes
        fig.update_xaxes(title_text="Revenue (M USD)", row=1, col=1)
        fig.update_xaxes(title_text="Genre", row=1, col=2, tickangle=-45)
        fig.update_xaxes(title_text="Budget (M USD)", row=2, col=1)
        fig.update_xaxes(title_text="Year", row=3, col=1)
        
        fig.update_yaxes(title_text="Count", row=1, col=1)
        fig.update_yaxes(title_text="Count", row=1, col=2)
        fig.update_yaxes(title_text="Revenue (M USD)", row=2, col=1)
        fig.update_yaxes(title_text="Rating", row=2, col=2)
        fig.update_yaxes(title_text="Revenue (M USD)", row=3, col=1)
        fig.update_yaxes(title_text="ROI (%)", row=3, col=2)
        
        output_path = self.viz_dir / output_file
        fig.write_html(str(output_path))
        
        logger.info(f"Comprehensive dashboard saved to {output_path}")
        return str(output_path)
    
    def generate_all_interactive_visualizations(
        self,
        df: DataFrame,
        kpis: Dict[str, Any]
    ) -> Dict[str, str]:
        """Generate all interactive Plotly visualizations"""
        logger.info("Generating all interactive visualizations")
        
        visualizations = {}
        
        try:
            visualizations['scatter'] = self.create_revenue_budget_scatter(df)
            visualizations['sunburst'] = self.create_genre_sunburst(df)
            visualizations['timeline'] = self.create_timeline_animation(df)
            visualizations['roi_box'] = self.create_roi_box_plot(df)
            visualizations['3d_scatter'] = self.create_3d_performance_scatter(df)
            visualizations['treemap'] = self.create_top_performers_treemap(df)
            visualizations['heatmap'] = self.create_rating_heatmap(df)
            visualizations['comprehensive'] = self.create_comprehensive_dashboard(df, kpis)
            
            logger.info(f"Generated {len(visualizations)} interactive visualizations")
            
        except Exception as e:
            logger.error(f"Error generating interactive visualizations: {str(e)}")
            raise
        
        return visualizations
