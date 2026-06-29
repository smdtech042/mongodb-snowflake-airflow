import os
import sys
import logging
import tempfile
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Ensure the include directory is in the import path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../include')))
try:
    from schema_mappings import MONGO_DATABASE
except ImportError:
    MONGO_DATABASE = "supplier_db"

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='cleanup',
    default_args=default_args,
    description='Performs lifecycle cleanup on Airflow workers and Snowflake stages',
    schedule_interval='@weekly', # Staging cleanup can run weekly or after main runs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cleanup', 'maintenance', 'snowflake'],
    params={
        'snowflake_conn_id': 'snowflake_default',
        'snowflake_database': 'MONGODB_RAW',
        'snowflake_schema': 'PUBLIC',
    }
)
def system_cleanup_dag():

    @task(task_id="cleanup_snowflake_stage")
    def cleanup_snowflake_stage(**context):
        """
        Removes all uploaded files from the Snowflake internal stage to save storage costs.
        """
        params = context['params']
        sf_conn = params['snowflake_conn_id']
        database = params['snowflake_database']
        schema = params['snowflake_schema']

        sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)
        
        # Check if stage exists before cleaning it up
        stage_check_sql = f"""
        SELECT COUNT(*) 
        FROM {database}.INFORMATION_SCHEMA.STAGES 
        WHERE STAGE_NAME = 'MONGODB_STAGE' 
        AND STAGE_SCHEMA = '{schema}';
        """
        stage_exists = sf_hook.get_first(stage_check_sql)[0] > 0

        if stage_exists:
            logger.info("Cleaning up files in MONGODB_STAGE...")
            cleanup_sql = f"REMOVE @{database}.{schema}.mongodb_stage;"
            sf_hook.run(cleanup_sql, autocommit=True)
            logger.info("Staged files removed successfully.")
        else:
            logger.info("MONGODB_STAGE does not exist, skipping stage cleanup.")

    @task(task_id="cleanup_local_temp_files")
    def cleanup_local_temp_files():
        """
        Deletes any stray mongo_export_* files from the local OS temp directory.
        """
        temp_dir = tempfile.gettempdir()
        logger.info(f"Scanning temp directory: {temp_dir} for temporary export files...")
        
        removed_count = 0
        for item in os.listdir(temp_dir):
            if item.startswith("mongo_export_") and item.endswith(".json"):
                file_path = os.path.join(temp_dir, item)
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted temporary export file: {file_path}")
                    removed_count += 1
                except Exception as e:
                    logger.warning(f"Could not delete temporary file {file_path}: {str(e)}")

        logger.info(f"Completed local cleanup. Removed {removed_count} temporary files.")

    clean_stage = cleanup_snowflake_stage()
    clean_local = cleanup_local_temp_files()

    clean_stage >> clean_local

# Instantiation
cleanup_dag = system_cleanup_dag()
