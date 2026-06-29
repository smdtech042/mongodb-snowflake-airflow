import os
import sys
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Ensure the include directory is in the import path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../include')))
try:
    from schema_mappings import SCHEMA_MAPPINGS, MONGO_DATABASE
except ImportError:
    MONGO_DATABASE = "supplier_db"
    SCHEMA_MAPPINGS = {}

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
    dag_id='validation',
    default_args=default_args,
    description='Validates record counts and schemas between MongoDB and Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['validation', 'mongodb', 'snowflake'],
    params={
        'mongo_conn_id': 'mongo_default',
        'snowflake_conn_id': 'snowflake_default',
        'snowflake_database': 'MONGODB_RAW',
        'snowflake_schema': 'PUBLIC',
        'mongo_db_name': MONGO_DATABASE,
    }
)
def data_validation_dag():

    # Dynamic Task Group for validating each collection
    def create_validation_tasks(collection_name, mapping):
        raw_table = mapping["table_name"]
        view_name = mapping["view_name"]

        @task(task_id=f"validate_{collection_name}")
        def validate_collection(collection: str, table: str, view: str, **context):
            params = context['params']
            mongo_conn = params['mongo_conn_id']
            sf_conn = params['snowflake_conn_id']
            database = params['snowflake_database']
            schema = params['snowflake_schema']
            mongo_db = params['mongo_db_name']

            # 1. Fetch Mongo Count
            mongo_hook = MongoHook(mongo_conn_id=mongo_conn)
            client = mongo_hook.get_conn()
            db = client[mongo_db]
            mongo_count = db[collection].count_documents({})
            logger.info(f"MongoDB Collection '{collection}' Document Count: {mongo_count}")

            # 2. Fetch Snowflake Raw Table Count
            sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)
            
            # Check if table exists
            table_check_sql = f"""
            SELECT COUNT(*) 
            FROM {database}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = '{table}' 
            AND TABLE_SCHEMA = '{schema}';
            """
            table_exists = sf_hook.get_first(table_check_sql)[0] > 0

            if not table_exists:
                logger.warning(f"Snowflake Raw Table '{table}' does not exist. Validation failed.")
                return {"collection": collection, "status": "FAIL", "reason": "Table does not exist"}

            sf_raw_count_sql = f"SELECT COUNT(*) FROM {database}.{schema}.{table}"
            sf_raw_count = sf_hook.get_first(sf_raw_count_sql)[0]
            logger.info(f"Snowflake Raw Table '{table}' Row Count: {sf_raw_count}")

            # 3. Test query the view
            view_check_sql = f"""
            SELECT COUNT(*) 
            FROM {database}.INFORMATION_SCHEMA.VIEWS 
            WHERE TABLE_NAME = '{view}' 
            AND TABLE_SCHEMA = '{schema}';
            """
            view_exists = sf_hook.get_first(view_check_sql)[0] > 0

            view_status = "OK"
            if view_exists:
                try:
                    # Run a simple check query against view
                    sf_view_check = f"SELECT * FROM {database}.{schema}.{view} LIMIT 1"
                    sf_hook.run(sf_view_check)
                    logger.info(f"Snowflake Relational View '{view}' is fully functional and queryable.")
                except Exception as e:
                    logger.error(f"Snowflake Relational View '{view}' query failed: {str(e)}")
                    view_status = f"ERROR: {str(e)}"
            else:
                logger.warning(f"Snowflake View '{view}' does not exist.")
                view_status = "MISSING"

            # 4. Compare Counts
            diff = abs(mongo_count - sf_raw_count)
            status = "PASS"
            if diff > 0:
                logger.warning(f"Count mismatch for '{collection}': MongoDB={mongo_count}, Snowflake={sf_raw_count} (Diff={diff})")
                status = "WARN"
            else:
                logger.info(f"Counts match perfectly for '{collection}'!")

            return {
                "collection": collection,
                "mongo_count": mongo_count,
                "sf_raw_count": sf_raw_count,
                "diff": diff,
                "status": status,
                "view_status": view_status
            }

        return validate_collection(collection_name, raw_table, view_name)

    with TaskGroup("validate_collections") as validate_group:
        for collection, mapping in SCHEMA_MAPPINGS.items():
            create_validation_tasks(collection, mapping)

    @task(task_id="summary_report")
    def generate_summary(validation_results, **context):
        """
        Gathers all task validation results and writes a neat markdown summary.
        """
        logger.info("Gathering validation summary...")
        mismatches = []
        errors = []
        passed = []
        
        # In Airflow 2.x, when using dynamic lists of task outputs, validation_results is a list
        for result in validation_results:
            if not result:
                continue
            
            col = result.get("collection")
            status = result.get("status")
            view_status = result.get("view_status")
            
            if status == "FAIL" or "ERROR" in str(view_status):
                errors.append(result)
            elif status == "WARN":
                mismatches.append(result)
            else:
                passed.append(result)

        logger.info("=== DATA QUALITY REPORT ===")
        logger.info(f"Total Collections Passed: {len(passed)}")
        logger.info(f"Total Collections with Warnings (Mismatches): {len(mismatches)}")
        logger.info(f"Total Collections Failed / Errors: {len(errors)}")

        if mismatches:
            logger.info("Mismatched collections detailed report:")
            for item in mismatches:
                logger.info(f" - {item['collection']}: MongoDB={item['mongo_count']}, Snowflake={item['sf_raw_count']} (diff={item['diff']})")

        if errors:
            logger.error("Errored/Failed collections detailed report:")
            for item in errors:
                logger.error(f" - {item.get('collection')}: Reason/View Status = {item.get('reason') or item.get('view_status')}")
            raise ValueError("Validation failed on one or more collections. Review task logs for details.")

    # Execute validation tasks in parallel, then compile summary
    results = [create_validation_tasks(col, map_cfg) for col, map_cfg in SCHEMA_MAPPINGS.items()]
    generate_summary(results)

# Instantiation
validation_dag = data_validation_dag()
