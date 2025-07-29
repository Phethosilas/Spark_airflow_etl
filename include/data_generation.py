"""
Data Generation Task
Handles Spark-based data generation and loading to PostgreSQL
"""

from include.spark_utils import get_spark_session
from include.database_connection import get_database_connection_properties
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import logging
import time

def generate_and_load_data_task(num_records, table_name="weather_data", data_size="medium"):
    """
    Generate and load data using Spark
    
    Args:
        num_records: Number of records to generate
        table_name: Target table name
        data_size: Data size category for Spark optimization
    
    Returns:
        Number of records loaded
    """
    start_time = time.time()
    logging.info(f"Starting Spark data generation for {num_records:,} records...")
    
    try:
        # Get Spark session with appropriate configuration
        spark = get_spark_session(data_size)
        
        # Optimized partitioning based on data size and available cores
        if num_records >= 100_000_000:  # 100M+ records
            optimal_partitions = min(spark.sparkContext.defaultParallelism * 8, 2000)
        elif num_records >= 10_000_000:  # 10M+ records
            optimal_partitions = spark.sparkContext.defaultParallelism * 4
        elif num_records >= 1_000_000:  # 1M+ records
            optimal_partitions = spark.sparkContext.defaultParallelism * 2
        else:
            optimal_partitions = spark.sparkContext.defaultParallelism
        
        num_partitions = optimal_partitions
        
        logging.info(f"Using {num_partitions} partitions for {num_records:,} records")
        
        # Generate data using Spark
        base_df = spark.range(0, num_records, numPartitions=num_partitions)
        
        # Add weather data columns
        weather_data = base_df.select(
            # Geographic coordinates
            (F.rand() * 180 - 90).cast(DoubleType()).alias("latitude"),
            (F.rand() * 360 - 180).cast(DoubleType()).alias("longitude"),
            
            # Weather measurements
            (F.rand() * 80 - 30).cast(DoubleType()).alias("temperature"),
            (F.rand() * 150).cast(DoubleType()).alias("windspeed"),
            (F.rand() * 360).cast(IntegerType()).alias("winddirection"),
            (F.rand() * 100).cast(IntegerType()).alias("weathercode"),
            
            # Boolean and time fields
            (F.rand() > 0.5).alias("is_day"),
            F.current_timestamp().alias("event_time"),
            
            # Batch tracking
            (F.col("id") % 1000).alias("batch_id")
        )
        
        # Get database connection properties
        db_props = get_database_connection_properties()
        
        logging.info("Writing data to PostgreSQL...")
        
        # Write to database
        weather_data.write \
            .format("jdbc") \
            .option("url", db_props["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_props["user"]) \
            .option("password", db_props["password"]) \
            .option("driver", db_props["driver"]) \
            .option("batchsize", db_props["batchsize"]) \
            .option("numPartitions", db_props["numPartitions"]) \
            .mode("append") \
            .save()
        
        elapsed_time = time.time() - start_time
        records_per_second = num_records / elapsed_time
        
        logging.info(f"Data loading completed!")
        logging.info(f"Records loaded: {num_records:,}")
        logging.info(f"Time taken: {elapsed_time:.2f} seconds")
        logging.info(f"Loading rate: {records_per_second:,.0f} records/second")
        
        return num_records
        
    except Exception as e:
        logging.error(f"Data generation and loading failed: {str(e)}")
        raise
