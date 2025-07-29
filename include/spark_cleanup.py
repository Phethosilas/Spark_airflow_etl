"""
Spark Cleanup Task
Handles Spark session cleanup and resource management
"""

from include.spark_utils import stop_spark_session
import logging

def cleanup_spark_resources():
    """Clean up Spark session and resources"""
    try:
        logging.info("Cleaning up Spark session...")
        stop_spark_session()
        logging.info("Spark session cleanup completed")
    except Exception as e:
        logging.warning(f"Spark session cleanup warning: {str(e)}")
