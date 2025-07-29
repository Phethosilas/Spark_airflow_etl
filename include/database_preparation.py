"""
Database Preparation Task
Handles PostgreSQL table creation and optimization
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
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
