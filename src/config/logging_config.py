"""
Logging Configuration Module
"""

import os
import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from datetime import datetime


class LoggerConfig:
    """Centralized logging configuration"""
    
    DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
    DEFAULT_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    
    @staticmethod
    def setup_logger(
        name: str,
        log_level: str = 'INFO',
        log_dir: Optional[str] = None,
        log_file: Optional[str] = None,
        console_output: bool = True,
        file_output: bool = True,
        max_bytes: int = 10485760,  # 10MB
        backup_count: int = 5
    ) -> logging.Logger:
        """
        Setup and configure logger
        
        Args:
            name: Logger name
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_dir: Directory for log files
            log_file: Specific log file name
            console_output: Enable console output
            file_output: Enable file output
            max_bytes: Maximum size of log file before rotation
            backup_count: Number of backup files to keep
            
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers
        logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            LoggerConfig.DEFAULT_FORMAT,
            datefmt=LoggerConfig.DEFAULT_DATE_FORMAT
        )
        
        # Console handler
        if console_output:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(getattr(logging, log_level.upper()))
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # File handler
        if file_output:
            if log_dir is None:
                log_dir = os.getenv('LOG_DIR', './logs')
            
            # Ensure log directory exists
            Path(log_dir).mkdir(parents=True, exist_ok=True)
            
            if log_file is None:
                log_file = f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
            
            log_path = os.path.join(log_dir, log_file)
            
            # Rotating file handler
            file_handler = logging.handlers.RotatingFileHandler(
                log_path,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
            file_handler.setLevel(getattr(logging, log_level.upper()))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        logger.info(f"Logger '{name}' configured successfully")
        return logger
    
    @staticmethod
    def setup_pipeline_logger(
        component: str,
        run_id: Optional[str] = None
    ) -> logging.Logger:
        """
        Setup logger for pipeline components
        
        Args:
            component: Pipeline component name
            run_id: Unique run identifier
            
        Returns:
            Configured logger
        """
        log_dir = os.getenv('LOG_DIR', './logs/pipeline')
        
        if run_id:
            log_file = f"{component}_{run_id}.log"
        else:
            log_file = f"{component}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        return LoggerConfig.setup_logger(
            name=f"tmdb_pipeline.{component}",
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            log_dir=log_dir,
            log_file=log_file,
            console_output=True,
            file_output=True
        )
    
    @staticmethod
    def setup_spark_logger() -> logging.Logger:
        """Setup logger for Spark operations"""
        return LoggerConfig.setup_logger(
            name='spark',
            log_level='WARN',  # Spark is verbose
            log_dir='./logs/spark',
            console_output=False,
            file_output=True
        )
    
    @staticmethod
    def setup_api_logger() -> logging.Logger:
        """Setup logger for API operations"""
        return LoggerConfig.setup_logger(
            name='tmdb_api',
            log_level='INFO',
            log_dir='./logs/api',
            console_output=True,
            file_output=True
        )
    
    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """Get or create logger with default configuration"""
        logger = logging.getLogger(name)
        
        if not logger.handlers:
            return LoggerConfig.setup_logger(name)
        
        return logger


class PerformanceLogger:
    """Logger for performance metrics"""
    
    def __init__(self, component: str):
        self.component = component
        self.logger = LoggerConfig.get_logger(f"performance.{component}")
        self.start_time = None
        self.metrics = {}
    
    def start(self, operation: str) -> None:
        """Start timing an operation"""
        self.start_time = datetime.now()
        self.logger.info(f"Starting {operation}")
    
    def end(self, operation: str, **kwargs) -> None:
        """End timing an operation"""
        if self.start_time:
            elapsed = (datetime.now() - self.start_time).total_seconds()
            self.logger.info(
                f"Completed {operation} in {elapsed:.2f}s",
                extra={**kwargs, 'elapsed_seconds': elapsed}
            )
            self.metrics[operation] = elapsed
            self.start_time = None
    
    def log_metric(self, metric_name: str, value: float, **kwargs) -> None:
        """Log a custom metric"""
        self.logger.info(
            f"Metric: {metric_name} = {value}",
            extra={**kwargs, 'metric_name': metric_name, 'metric_value': value}
        )
        self.metrics[metric_name] = value
    
    def get_metrics(self) -> dict:
        """Get all collected metrics"""
        return self.metrics.copy()


class AuditLogger:
    """Logger for audit trail"""
    
    def __init__(self):
        self.logger = LoggerConfig.setup_logger(
            name='audit',
            log_level='INFO',
            log_dir='./logs/audit',
            console_output=False,
            file_output=True
        )
    
    def log_data_access(
        self,
        user: str,
        resource: str,
        action: str,
        status: str = 'success'
    ) -> None:
        """Log data access event"""
        self.logger.info(
            f"Data Access: {user} performed {action} on {resource} - {status}",
            extra={
                'event_type': 'data_access',
                'user': user,
                'resource': resource,
                'action': action,
                'status': status
            }
        )
    
    def log_pipeline_run(
        self,
        run_id: str,
        stage: str,
        status: str,
        **kwargs
    ) -> None:
        """Log pipeline run event"""
        self.logger.info(
            f"Pipeline Run: {run_id} - {stage} - {status}",
            extra={
                'event_type': 'pipeline_run',
                'run_id': run_id,
                'stage': stage,
                'status': status,
                **kwargs
            }
        )
    
    def log_data_quality(
        self,
        dataset: str,
        quality_score: float,
        issues: list,
        **kwargs
    ) -> None:
        """Log data quality event"""
        self.logger.info(
            f"Data Quality: {dataset} - Score: {quality_score}",
            extra={
                'event_type': 'data_quality',
                'dataset': dataset,
                'quality_score': quality_score,
                'issues': issues,
                **kwargs
            }
        )


# Convenience functions
def get_logger(name: str) -> logging.Logger:
    """Get logger instance"""
    return LoggerConfig.get_logger(name)


def setup_root_logger(log_level: str = 'INFO') -> None:
    """Setup root logger for the application"""
    LoggerConfig.setup_logger(
        name='tmdb_pipeline',
        log_level=log_level,
        log_dir='./logs',
        console_output=True,
        file_output=True
    )