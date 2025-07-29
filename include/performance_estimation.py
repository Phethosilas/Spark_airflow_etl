"""
Performance Estimation Task
Provides performance estimates based on data size and system resources
"""

from include.spark_utils import estimate_processing_time
import logging

def estimate_performance_task(num_records, data_size="medium"):
    """
    Estimate performance based on parameters
    
    Args:
        num_records: Number of records to process
        data_size: Data size category
    
    Returns:
        Dictionary with performance estimates
    """
    estimates = estimate_processing_time(num_records, data_size)
    
    logging.info("PERFORMANCE ESTIMATION")
    logging.info("=" * 50)
    logging.info(f"Records to process: {num_records:,}")
    logging.info(f"Data size category: {data_size}")
    logging.info(f"Estimated rate: {estimates['estimated_rate_per_sec']:,} records/sec")
    logging.info(f"Estimated time: {estimates['estimated_time_formatted']}")
    
    return estimates
