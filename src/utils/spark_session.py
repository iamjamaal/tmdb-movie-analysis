"""
Spark Session Management Utilities
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """Manages Spark session creation and configuration"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_or_create_session(
        cls,
        app_name: str = "TMDB Movie Analysis",
        config: Optional[Dict[str, Any]] = None
    ) -> SparkSession:
        """Get existing or create new Spark session"""
        
        if cls._instance is None:
            logger.info(f"Creating new Spark session: {app_name}")
            cls._instance = cls._create_session(app_name, config or {})
        else:
            logger.info("Reusing existing Spark session")
            
        return cls._instance
    
    @classmethod
    def _create_session(
        cls,
        app_name: str,
        config: Dict[str, Any]
    ) -> SparkSession:
        """Create and configure Spark session"""
        
        # Build Spark configuration
        spark_conf = SparkConf()
        
        # Default configurations
        default_config = {
            'spark.app.name': app_name,
            'spark.master': config.get('master', 'local[*]'),
            'spark.driver.memory': config.get('driver_memory', '4g'),
            'spark.executor.memory': config.get('executor_memory', '4g'),
            'spark.executor.cores': config.get('executor_cores', '2'),
            'spark.sql.shuffle.partitions': config.get('shuffle_partitions', '200'),
            'spark.default.parallelism': config.get('default_parallelism', '8'),
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.kryoserializer.buffer.max': '512m',
            'spark.sql.sources.partitionOverwriteMode': 'dynamic',
            'spark.sql.parquet.compression.codec': 'snappy',
            'spark.sql.warehouse.dir': config.get('warehouse_dir', '/opt/spark-data/warehouse')
        }
        
        # Override with custom config
        custom_spark_config = config.get('spark', {})
        default_config.update(custom_spark_config)
        
        # Set all configurations
        for key, value in default_config.items():
            spark_conf.set(key, value)
        
        # Create Spark session
        builder = SparkSession.builder.config(conf=spark_conf)
        
        # Enable Hive support if needed
        if config.get('enable_hive', False):
            builder = builder.enableHiveSupport()
        
        session = builder.getOrCreate()
        
        # Set log level
        log_level = config.get('log_level', 'WARN')
        session.sparkContext.setLogLevel(log_level)
        
        logger.info(f"Spark session created successfully")
        logger.info(f"Spark version: {session.version}")
        logger.info(f"Master: {session.sparkContext.master}")
        
        return session
    
    @classmethod
    def stop_session(cls) -> None:
        """Stop the Spark session"""
        if cls._instance is not None:
            logger.info("Stopping Spark session")
            cls._instance.stop()
            cls._instance = None
        else:
            logger.info("No active Spark session to stop")
    
    @classmethod
    def configure_checkpoint_dir(cls, checkpoint_dir: str) -> None:
        """Configure checkpoint directory for fault tolerance"""
        if cls._instance:
            cls._instance.sparkContext.setCheckpointDir(checkpoint_dir)
            logger.info(f"Checkpoint directory set to: {checkpoint_dir}")
    
    @classmethod
    def get_session_info(cls) -> Dict[str, Any]:
        """Get information about current Spark session"""
        if cls._instance is None:
            return {'status': 'No active session'}
        
        sc = cls._instance.sparkContext
        conf = sc.getConf()
        
        return {
            'status': 'Active',
            'app_name': sc.appName,
            'app_id': sc.applicationId,
            'master': sc.master,
            'spark_version': cls._instance.version,
            'python_version': sc.pythonVer,
            'default_parallelism': sc.defaultParallelism,
            'configuration': dict(conf.getAll())
        }


def create_optimized_session(
    app_name: str,
    environment: str = "development"
) -> SparkSession:
    """Create Spark session with environment-specific optimizations"""
    
    configs = {
        'development': {
            'master': 'local[*]',
            'driver_memory': '2g',
            'executor_memory': '2g',
            'shuffle_partitions': '50',
            'log_level': 'INFO'
        },
        'testing': {
            'master': 'local[2]',
            'driver_memory': '1g',
            'executor_memory': '1g',
            'shuffle_partitions': '10',
            'log_level': 'WARN'
        },
        'production': {
            'master': 'spark://spark-master:7077',
            'driver_memory': '4g',
            'executor_memory': '4g',
            'executor_cores': '4',
            'shuffle_partitions': '200',
            'log_level': 'ERROR'
        }
    }
    
    config = configs.get(environment, configs['development'])
    return SparkSessionManager.get_or_create_session(app_name, config)