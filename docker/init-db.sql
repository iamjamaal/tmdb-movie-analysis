-- Create additional database for analytics
CREATE DATABASE tmdb_analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE tmdb_analytics TO airflow;