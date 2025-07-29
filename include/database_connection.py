"""
Database Connection Utilities
Handles PostgreSQL connection properties for Spark JDBC operations
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

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
            "batchsize": "50000",
            "numPartitions": "20",
            "rewriteBatchedStatements": "true"
        }
    except Exception as e:
        logging.error(f"Failed to get database connection properties: {str(e)}")
        raise
