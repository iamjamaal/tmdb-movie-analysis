"""
Utility Helper Functions
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    logger = logging.getLogger(__name__)
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration: {str(e)}")
        raise


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = None
) -> None:
    """Setup logging configuration"""
    
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handlers = [logging.StreamHandler()]
    
    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        handlers=handlers
    )


def ensure_directory(directory: str) -> None:
    """Ensure directory exists, create if not"""
    Path(directory).mkdir(parents=True, exist_ok=True)


def get_environment() -> str:
    """Get current environment from environment variable"""
    return os.getenv('ENVIRONMENT', 'development')


def get_api_key() -> str:
    """Get TMDB API key from environment"""
    api_key = os.getenv('TMDB_API_KEY')
    if not api_key:
        raise ValueError("TMDB_API_KEY environment variable not set")
    return api_key


def format_currency(value: float, precision: int = 2) -> str:
    """Format currency value"""
    return f"${value:,.{precision}f}M"


def format_percentage(value: float, precision: int = 2) -> str:
    """Format percentage value"""
    return f"{value:.{precision}f}%"


def calculate_roi(revenue: float, budget: float) -> Optional[float]:
    """Calculate Return on Investment"""
    if budget and budget > 0:
        return round((revenue / budget - 1) * 100, 2)
    return None


def calculate_profit(revenue: float, budget: float) -> float:
    """Calculate profit"""
    return round(revenue - budget, 2)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide two numbers"""
    if denominator and denominator != 0:
        return numerator / denominator
    return default


def parse_date(date_string: str, fmt: str = "%Y-%m-%d") -> Optional[datetime]:
    """Parse date string to datetime object"""
    try:
        return datetime.strptime(date_string, fmt)
    except (ValueError, TypeError):
        return None


def get_year_from_date(date_obj: Optional[datetime]) -> Optional[int]:
    """Extract year from datetime object"""
    return date_obj.year if date_obj else None


def chunk_list(items: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split list into chunks"""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Flatten nested dictionary"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def sanitize_filename(filename: str) -> str:
    """Sanitize filename by removing invalid characters"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    return filename


def get_file_size(file_path: str) -> int:
    """Get file size in bytes"""
    return os.path.getsize(file_path) if os.path.exists(file_path) else 0


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def get_timestamp() -> str:
    """Get current timestamp as string"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def create_run_id() -> str:
    """Create unique run identifier"""
    return f"run_{get_timestamp()}"


def retry_on_failure(max_retries: int = 3, delay: int = 2):
    """Decorator for retrying function on failure"""
    import time
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"Failed after {max_retries} attempts: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempt + 1} failed, retrying in {delay}s: {str(e)}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator


def measure_execution_time(func):
    """Decorator to measure function execution time"""
    import functools
    
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__module__)
        start_time = datetime.now()
        logger.info(f"Starting {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(f"Completed {func.__name__} in {elapsed:.2f}s")
            return result
        except Exception as e:
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.error(f"Failed {func.__name__} after {elapsed:.2f}s: {str(e)}")
            raise
    
    return wrapper


class ProgressTracker:
    """Track progress of long-running operations"""
    
    def __init__(self, total: int, description: str = "Processing"):
        self.total = total
        self.current = 0
        self.description = description
        self.start_time = datetime.now()
        self.logger = logging.getLogger(__name__)
        
    def update(self, increment: int = 1) -> None:
        """Update progress"""
        self.current += increment
        percentage = (self.current / self.total * 100) if self.total > 0 else 0
        
        if self.current % max(1, self.total // 10) == 0 or self.current == self.total:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            rate = self.current / elapsed if elapsed > 0 else 0
            eta = (self.total - self.current) / rate if rate > 0 else 0
            
            self.logger.info(
                f"{self.description}: {self.current}/{self.total} "
                f"({percentage:.1f}%) - "
                f"Rate: {rate:.2f} items/s - "
                f"ETA: {eta:.1f}s"
            )
    
    def complete(self) -> None:
        """Mark as complete"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        self.logger.info(
            f"{self.description} completed: {self.total} items in {elapsed:.2f}s "
            f"({self.total/elapsed:.2f} items/s)"
        )


def validate_environment_variables(required_vars: List[str]) -> None:
    """Validate that required environment variables are set"""
    logger = logging.getLogger(__name__)
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        raise EnvironmentError(error_msg)
    
    logger.info("All required environment variables are set")


def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple dictionaries"""
    result = {}
    for d in dicts:
        result.update(d)
    return result


def filter_none_values(d: Dict[str, Any]) -> Dict[str, Any]:
    """Remove None values from dictionary"""
    return {k: v for k, v in d.items() if v is not None}