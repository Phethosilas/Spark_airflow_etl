"""
Database Tasks for Flexible ETL
Handles PostgreSQL database operations for any data size
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.spark_utils import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
import logging
import time

def prepare_database_task(table_name="weather_data"):
    """
    Create optimized table structure
    
    Args:
        table_name: Name of the table to create
    
    Returns: Boolean indicating success
    """
    start_time = time.time()
    logging.info(f"Starting database preparation for table: {table_name}")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                logging.info(f"Dropping existing table {table_name} if exists...")
                
                cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                
                logging.info(f"Creating table {table_name}...")
                
                cur.execute(f"""
                    CREATE TABLE {table_name} (
                        id BIGSERIAL PRIMARY KEY,
                        latitude NUMERIC(8,6),
                        longitude NUMERIC(9,6),
                        temperature NUMERIC(3,1),
                        windspeed NUMERIC(4,1),
                        winddirection SMALLINT,
                        weathercode SMALLINT,
                        is_day BOOLEAN,
                        event_time TIMESTAMPTZ,
                        batch_id INTEGER,
                        loaded_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                
                conn.commit()
                
                # Verify table creation
                cur.execute("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                
                columns = cur.fetchall()
                logging.info(f"Table {table_name} created successfully with {len(columns)} columns")
        
        elapsed_time = time.time() - start_time
        logging.info(f"Database preparation completed in {elapsed_time:.2f} seconds")
        
        return True
        
    except Exception as e:
        logging.error(f"Database preparation failed: {str(e)}")
        raise

def get_database_connection_properties():
    """
    Get database connection properties for Spark JDBC operations
    Returns: Dictionary with connection properties
    """
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_connection('postgres_default')
        
        return {
            "url": f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}",
            "user": conn.login,
            "password": conn.password,
            "driver": "org.postgresql.Driver",
            "batchsize": "10000",
            "numPartitions": "10",
            "rewriteBatchedStatements": "true"
        }
    except Exception as e:
        logging.error(f"Failed to get database connection properties: {str(e)}")
        raise

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
        
        # Calculate partitioning based on data size
        if num_records < 100_000:
            records_per_partition = 10_000
        elif num_records < 10_000_000:
            records_per_partition = 50_000
        else:
            records_per_partition = 100_000
            
        num_partitions = max(num_records // records_per_partition, spark.sparkContext.defaultParallelism)
        
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

def finalize_database_task(loaded_records, table_name="weather_data"):
    """
    Finalize database with indexing and statistics
    
    Args:
        loaded_records: Number of records that were loaded
        table_name: Name of the table to finalize
    
    Returns:
        Dictionary with final statistics
    """
    start_time = time.time()
    logging.info(f"Starting database finalization for {table_name}...")
    
    try:
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                logging.info("Creating indexes...")
                
                # Create useful indexes
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_batch ON {table_name}(batch_id);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_time ON {table_name}(event_time);")
                cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_location ON {table_name}(latitude, longitude);")
                
                # Update table statistics
                logging.info("Updating table statistics...")
                cur.execute(f"ANALYZE {table_name};")
                
                # Get final statistics
                cur.execute(f"""
                    SELECT 
                        pg_size_pretty(pg_total_relation_size('{table_name}')) as table_size,
                        count(*) as record_count,
                        min(event_time) as earliest_event,
                        max(event_time) as latest_event,
                        avg(temperature)::numeric(5,2) as avg_temperature
                    FROM {table_name};
                """)
                
                stats = cur.fetchone()
                conn.commit()
        
        elapsed_time = time.time() - start_time
        
        logging.info("="*50)
        logging.info("DATABASE FINALIZATION COMPLETED")
        logging.info("="*50)
        logging.info(f"Finalization time: {elapsed_time:.2f} seconds")
        logging.info(f"Records processed: {loaded_records:,}")
        logging.info(f"Table size: {stats[0]}")
        logging.info(f"Record count: {stats[1]:,}")
        logging.info(f"Average temperature: {stats[4]}°C")
        
        return {
            "finalization_time_seconds": elapsed_time,
            "records_processed": loaded_records,
            "table_size": stats[0],
            "record_count": stats[1],
            "avg_temperature": float(stats[4]) if stats[4] else 0
        }
        
    except Exception as e:
        logging.error(f"Database finalization failed: {str(e)}")
        raise

def cleanup_spark_resources():
    """Clean up Spark session and resources"""
    try:
        from include.spark_utils import stop_spark_session
        logging.info("Cleaning up Spark session...")
        stop_spark_session()
        logging.info("Spark session cleanup completed")
    except Exception as e:
        logging.warning(f"Spark session cleanup warning: {str(e)}")
