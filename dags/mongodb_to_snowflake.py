import os
import sys
import json
import logging
from datetime import datetime, timedelta
import tempfile

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Ensure the include directory is in the import path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../include')))
try:
    from schema_mappings import SCHEMA_MAPPINGS, MONGO_DATABASE
except ImportError:
    # Fallback mappings in case of directory issues during compilation checks
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

def serialize_mongo_doc(doc):
    """
    Recursively converts MongoDB BSON types (ObjectId, datetime, Decimal128)
    into standard JSON-serializable types.
    """
    if isinstance(doc, dict):
        new_doc = {}
        for k, v in doc.items():
            if k == '_id':
                new_doc['_id'] = str(v)
            else:
                new_doc[k] = serialize_mongo_doc(v)
        return new_doc
    elif isinstance(doc, list):
        return [serialize_mongo_doc(x) for x in doc]
    elif hasattr(doc, 'isoformat'): # DateTime/Timestamp objects
        return doc.isoformat()
    elif hasattr(doc, '__str__') and doc.__class__.__name__ in ('ObjectId', 'Decimal128'):
        return str(doc)
    else:
        return doc

def to_snake_case(camel_str):
    """
    Converts camelCase to SNAKE_CASE for standard relational column names.
    """
    snake = "".join(["_" + c.upper() if c.isupper() else c.upper() for c in camel_str])
    if snake.startswith("_"):
        snake = snake[1:]
    return snake

@dag(
    dag_id='mongodb_to_snowflake',
    default_args=default_args,
    description='ELT pipeline to migrate MongoDB data to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['mongodb', 'snowflake', 'elt'],
    params={
        'mongo_conn_id': 'mongo_default',
        'snowflake_conn_id': 'snowflake_default',
        'snowflake_database': 'MONGODB_RAW',
        'snowflake_schema': 'PUBLIC',
        'snowflake_warehouse': 'COMPUTE_WH',
        'mongo_db_name': MONGO_DATABASE,
        'batch_size': 5000,
    }
)
def mongodb_to_snowflake_etl():

    @task(task_id="initialize_snowflake_resources")
    def init_snowflake(**context):
        """
        Initializes target Database, Schema, and Internal Stage in Snowflake.
        """
        params = context['params']
        sf_conn = params['snowflake_conn_id']
        database = params['snowflake_database']
        schema = params['snowflake_schema']
        warehouse = params['snowflake_warehouse']

        sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)
        
        setup_queries = [
            f"CREATE DATABASE IF NOT EXISTS {database};",
            f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};",
            f"CREATE STAGE IF NOT EXISTS {database}.{schema}.mongodb_stage;",
        ]
        
        logger.info("Setting up database, schema, and internal stage...")
        for query in setup_queries:
            sf_hook.run(query, autocommit=True)

    # Dynamic Task Group for processing each collection in parallel
    def create_collection_tasks(collection_name, mapping):
        raw_table = mapping["table_name"]
        view_name = mapping["view_name"]
        fields = mapping["fields"]

        @task(task_id=f"elt_{collection_name}", multiple_outputs=True)
        def run_collection_elt(collection: str, table: str, view: str, fields_map: dict, **context):
            params = context['params']
            mongo_conn = params['mongo_conn_id']
            sf_conn = params['snowflake_conn_id']
            database = params['snowflake_database']
            schema = params['snowflake_schema']
            mongo_db = params['mongo_db_name']
            batch_size = int(params['batch_size'])

            # 1. Extract from MongoDB and write to local JSONL
            logger.info(f"Connecting to MongoDB and fetching from collection '{collection}'...")
            mongo_hook = MongoHook(mongo_conn_id=mongo_conn)
            client = mongo_hook.get_conn()
            db = client[mongo_db]
            cursor = db[collection].find()

            temp_dir = tempfile.gettempdir()
            filename = f"mongo_export_{collection}_{context['run_id']}.json"
            local_filepath = os.path.join(temp_dir, filename)

            record_count = 0
            logger.info(f"Writing records to temporary local file {local_filepath}...")
            with open(local_filepath, 'w', encoding='utf-8') as f:
                batch = []
                for doc in cursor:
                    serialized = serialize_mongo_doc(doc)
                    batch.append(json.dumps(serialized))
                    record_count += 1
                    
                    if len(batch) >= batch_size:
                        f.write("\n".join(batch) + "\n")
                        batch = []
                
                if batch:
                    f.write("\n".join(batch) + "\n")

            logger.info(f"Total extracted records: {record_count}")

            if record_count == 0:
                logger.info(f"No records found in collection '{collection}'. Skipping Snowflake load.")
                if os.path.exists(local_filepath):
                    os.remove(local_filepath)
                return {"records_loaded": 0, "status": "SKIPPED_EMPTY"}

            # 2. Setup Raw Table DDL
            sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)
            create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
                _MONGO_ID VARCHAR(128) PRIMARY KEY,
                DATA VARIANT,
                INGESTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
            );
            """
            sf_hook.run(create_table_ddl, autocommit=True)

            # 3. Create Transient Staging table for Merge
            temp_table = f"TEMP_{table}_{context['ds_nodash']}"
            create_temp_table_ddl = f"""
            CREATE OR REPLACE TEMPORARY TABLE {database}.{schema}.{temp_table} (
                _MONGO_ID VARCHAR(128),
                DATA VARIANT,
                INGESTED_AT TIMESTAMP_TZ
            );
            """
            sf_hook.run(create_temp_table_ddl, autocommit=True)

            # 4. Upload to Snowflake Internal Stage (PUT)
            logger.info(f"Uploading file to Snowflake stage: @{database}.{schema}.mongodb_stage...")
            conn = sf_hook.get_conn()
            sf_cursor = conn.cursor()
            try:
                # PUT requires clean UNIX style path separators
                clean_path = local_filepath.replace(os.sep, '/')
                put_sql = f"PUT 'file://{clean_path}' @{database}.{schema}.mongodb_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                sf_cursor.execute(put_sql)
            finally:
                sf_cursor.close()
                conn.close()

            # 5. COPY INTO Temp Table
            logger.info(f"Loading staged files into temp table {temp_table}...")
            copy_sql = f"""
            COPY INTO {database}.{schema}.{temp_table} (_MONGO_ID, DATA, INGESTED_AT)
            FROM (
                SELECT 
                    $1:_id::VARCHAR,
                    $1,
                    CURRENT_TIMESTAMP()
                FROM @{database}.{schema}.mongodb_stage/{filename}.gz
            )
            FILE_FORMAT = (TYPE = 'JSON')
            ON_ERROR = 'CONTINUE';
            """
            sf_hook.run(copy_sql, autocommit=True)

            # 6. MERGE into target RAW Table
            logger.info(f"Merging temp table {temp_table} into raw table {table}...")
            merge_sql = f"""
            MERGE INTO {database}.{schema}.{table} AS target
            USING {database}.{schema}.{temp_table} AS src
            ON target._MONGO_ID = src._MONGO_ID
            WHEN MATCHED THEN
                UPDATE SET target.DATA = src.DATA, target.INGESTED_AT = src.INGESTED_AT
            WHEN NOT MATCHED THEN
                INSERT (_MONGO_ID, DATA, INGESTED_AT)
                VALUES (src._MONGO_ID, src.DATA, src.INGESTED_AT);
            """
            sf_hook.run(merge_sql, autocommit=True)

            # 7. Create/Replace View representing Mongoose Schema fields
            logger.info(f"Deploying view {view}...")
            select_clauses = ["DATA:_id::VARCHAR AS MONGO_ID"]
            for field, sql_type in fields_map.items():
                col_name = to_snake_case(field)
                select_clauses.append(f"DATA:{field}::{sql_type} AS {col_name}")
            
            select_clauses.append("INGESTED_AT AS INGESTED_AT")
            
            view_ddl = f"""
            CREATE OR REPLACE VIEW {database}.{schema}.{view} AS
            SELECT
                {', '.join(select_clauses)}
            FROM {database}.{schema}.{table};
            """
            sf_hook.run(view_ddl, autocommit=True)

            # Cleanup Local Temp File
            if os.path.exists(local_filepath):
                os.remove(local_filepath)
                logger.info(f"Removed local temporary file {local_filepath}")

            return {"records_loaded": record_count, "status": "SUCCESS"}

        return run_collection_elt(collection_name, raw_table, view_name, fields)

    init_db = init_snowflake()

    # Create task groups for clear visualization in Airflow UI
    with TaskGroup("load_collections") as load_group:
        for collection, mapping in SCHEMA_MAPPINGS.items():
            create_collection_tasks(collection, mapping)

    init_db >> load_group

# Define the DAG instantiation
mongodb_to_snowflake_dag = mongodb_to_snowflake_etl()
