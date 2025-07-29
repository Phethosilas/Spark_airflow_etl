"""
Database Finalization Task
Handles database indexing, statistics, and final optimization
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import time

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
