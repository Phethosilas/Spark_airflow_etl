"""
Spark Session Utility for ETL
Provides Spark session management with configurable settings based on data size
"""

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import logging

# Global variable to store the Spark session
_spark_session = None

def get_spark_session(data_size="medium"):
    """
    Get or create Spark session with configurations based on expected data size
    
    Args:
        data_size: "small" (< 1M records), "medium" (1M-100M), "large" (100M-1B), "xlarge" (1B+)
    
    Returns:
        SparkSession
    """
    global _spark_session
    
    if _spark_session is None:
        logging.info(f"Creating Spark session optimized for {data_size} data size...")
        
        # Configuration based on data size
        configs = {
            "small": {
                "spark.executor.memory": "1g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
                "spark.sql.shuffle.partitions": "50",
                "spark.default.parallelism": "20"
            },
            "medium": {
                "spark.executor.memory": "2g",
                "spark.executor.cores": "2",
                "spark.driver.memory": "1g",
                "spark.sql.shuffle.partitions": "200",
                "spark.default.parallelism": "50"
            },
            "large": {
                "spark.executor.memory": "4g",
                "spark.executor.cores": "4",
                "spark.driver.memory": "2g",
                "spark.sql.shuffle.partitions": "400",
                "spark.default.parallelism": "100"
            },
            "xlarge": {
                "spark.executor.memory": "8g",
                "spark.executor.cores": "6",
                "spark.driver.memory": "4g",
                "spark.sql.shuffle.partitions": "1200",
                "spark.default.parallelism": "400",
                "spark.executor.instances": "4",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
                "spark.sql.adaptive.skewJoin.enabled": "true"
            }
        }
        
        # Get configuration for the specified data size
        size_config = configs.get(data_size, configs["medium"])
        
        # Base configuration with optimizations
        base_config = [
            ("spark.app.name", "FlexibleETL"),
            ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
            ("spark.sql.adaptive.enabled", "true"),
            ("spark.sql.adaptive.coalescePartitions.enabled", "true"),
            ("spark.sql.adaptive.localShuffleReader.enabled", "true"),
            ("spark.sql.execution.arrow.pyspark.enabled", "true"),
            ("spark.jars.packages", "org.postgresql:postgresql:42.7.1"),
            ("spark.sql.sources.parallelPartitionDiscovery.threshold", "32"),
        ]
        
        # Combine base config with size-specific config
        all_config = base_config + list(size_config.items())
        
        conf = SparkConf().setAll(all_config)
        
        _spark_session = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()
        
        _spark_session.sparkContext.setLogLevel("WARN")
        
        logging.info(f"Spark session created for {data_size} data processing")
        logging.info(f"Available cores: {_spark_session.sparkContext.defaultParallelism}")
        
    return _spark_session

def stop_spark_session():
    """Stop the Spark session"""
    global _spark_session
    
    if _spark_session is not None:
        logging.info("Stopping Spark session...")
        _spark_session.stop()
        _spark_session = None

def estimate_processing_time(num_records, data_size="medium"):
    """
    Estimate processing time based on record count and cluster size
    
    Args:
        num_records: Number of records to process
        data_size: Expected data size category
    
    Returns:
        Dictionary with estimates
    """
    # Optimized processing rates (records per second) by data size category
    rates = {
        "small": 15_000,    # Small setup (improved)
        "medium": 75_000,   # Medium setup (improved)
        "large": 300_000,   # Large setup (improved)
        "xlarge": 800_000   # Extra large setup (significantly improved)
    }
    
    rate = rates.get(data_size, rates["medium"])
    estimated_seconds = num_records / rate
    
    return {
        "data_size": data_size,
        "estimated_rate_per_sec": rate,
        "estimated_time_seconds": estimated_seconds,
        "estimated_time_hours": estimated_seconds / 3600,
        "estimated_time_formatted": f"{int(estimated_seconds // 3600)}:{int((estimated_seconds % 3600) // 60):02d}:{int(estimated_seconds % 60):02d}"
    }
