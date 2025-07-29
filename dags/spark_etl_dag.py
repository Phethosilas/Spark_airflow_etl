"""
Flexible Spark ETL DAG
Configurable Airflow DAG using Spark for loading any number of records into PostgreSQL
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

# Import our custom task functions
from include.database_preparation import prepare_database_task
from include.data_generation import generate_and_load_data_task
from include.database_finalization import finalize_database_task
from include.spark_cleanup import cleanup_spark_resources
from include.performance_estimation import estimate_performance_task

# DAG configuration
default_args = {
    'owner': 'data_engineering_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'spark_etl_pipeline',
    default_args=default_args,
    description='Flexible Spark ETL pipeline for any data size',
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['spark', 'postgres', 'etl', 'flexible'],
    params={
        "num_records": 1_000_000,  # Default: 1 million records
        "table_name": "weather_data",
        "data_size": "medium"  # small, medium, large, xlarge
    },
    doc_md="""
    # Flexible Spark ETL Pipeline
    
    This DAG can process any number of records using Apache Spark with PostgreSQL.
    
    ## Parameters
    - **num_records**: Number of records to generate (default: 1,000,000)
    - **table_name**: Target table name (default: weather_data)  
    - **data_size**: Spark optimization level - small/medium/large/xlarge (default: medium)
    
    ## Data Size Guidelines
    - **small**: < 1M records (1g executor memory)
    - **medium**: 1M-100M records (2g executor memory) 
    - **large**: 100M-1B records (4g executor memory)
    - **xlarge**: 1B+ records (8g executor memory)
    
    ## Usage Examples
    - Small test: 10,000 records with data_size="small"
    - Medium load: 10,000,000 records with data_size="medium"
    - Large load: 100,000,000 records with data_size="large"
    - Your original billion: 1,000,000,000 records with data_size="xlarge"
    """,
)
def spark_etl_pipeline():

    @task
    def estimate_performance(**context):
        """Estimate performance based on parameters"""
        params = context['params']
        num_records = params['num_records']
        data_size = params['data_size']
        
        return estimate_performance_task(num_records, data_size)

    @task
    def prepare_database(**context):
        """Prepare database with optimized settings"""
        params = context['params']
        table_name = params['table_name']
        
        logging.info(f"Preparing database table: {table_name}")
        
        result = prepare_database_task(table_name)
        
        if result:
            logging.info("Database preparation completed successfully")
            return {"status": "success", "table_name": table_name}
        else:
            raise Exception("Database preparation failed")

    @task
    def generate_and_load_data(db_prep_result, **context):
        """Generate and load data using Spark"""
        params = context['params']
        num_records = params['num_records']
        table_name = params['table_name']
        data_size = params['data_size']
        
        logging.info(f"Starting Spark data generation...")
        logging.info(f"Records: {num_records:,}")
        logging.info(f"Table: {table_name}")
        logging.info(f"Data size: {data_size}")
        
        loaded_records = generate_and_load_data_task(num_records, table_name, data_size)
        
        logging.info(f"Data loading completed. Records loaded: {loaded_records:,}")
        
        return {
            "status": "success",
            "records_loaded": loaded_records,
            "table_name": table_name
        }

    @task
    def finalize_database(load_result, **context):
        """Finalize database with indexes and statistics"""
        table_name = load_result['table_name']
        records_loaded = load_result['records_loaded']
        
        logging.info(f"Starting database finalization for {table_name}...")
        logging.info(f"Records to finalize: {records_loaded:,}")
        
        final_stats = finalize_database_task(records_loaded, table_name)
        
        logging.info("Database finalization completed successfully")
        
        return {
            "status": "success",
            "final_statistics": final_stats,
            "records_processed": records_loaded
        }

    @task(trigger_rule="all_done")
    def cleanup_and_report(finalization_result=None, **context):
        """Clean up resources and generate final report"""
        try:
            # Clean up Spark session
            cleanup_spark_resources()
            
            if finalization_result and finalization_result.get('status') == 'success':
                stats = finalization_result['final_statistics']
                records = finalization_result['records_processed']
                
                logging.info("\n" + "="*60)
                logging.info("SPARK ETL PIPELINE - FINAL REPORT")
                logging.info("="*60)
                
                logging.info(f"✅ Successfully processed {records:,} records")
                logging.info(f"📊 Final table size: {stats.get('table_size', 'Unknown')}")
                logging.info(f"📈 Average temperature: {stats.get('avg_temperature', 'Unknown')}°C")
                logging.info(f"⏱️  Finalization time: {stats.get('finalization_time_seconds', 0):.2f} seconds")
                
                return {
                    "status": "completed",
                    "message": "Spark ETL pipeline completed successfully",
                    "final_stats": stats
                }
            else:
                logging.warning("Pipeline completed with issues - check upstream task logs")
                return {
                    "status": "completed_with_warnings",
                    "message": "Pipeline completed but some tasks may have failed"
                }
                
        except Exception as e:
            logging.error(f"Cleanup and reporting failed: {str(e)}")
            return {
                "status": "cleanup_failed",
                "message": f"Cleanup failed: {str(e)}"
            }

    # Define task dependencies
    performance_est = estimate_performance()
    db_prep = prepare_database()
    data_load = generate_and_load_data(db_prep)
    db_final = finalize_database(data_load)
    cleanup = cleanup_and_report(db_final)

    # Set up the workflow
    performance_est >> db_prep >> data_load >> db_final >> cleanup

# Instantiate the DAG
spark_etl_pipeline()
