import os
import re
import sys
import json
import logging
from datetime import datetime, timedelta
import tempfile

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Ensure the include directory is in the import path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../include"))
)
try:
    from schema_mappings import SCHEMA_MAPPINGS, MONGO_DATABASE
except ImportError:
    # Fallback mappings in case of directory issues during compilation checks
    MONGO_DATABASE = "supplier_db"
    SCHEMA_MAPPINGS = {}

logger = logging.getLogger(__name__)

default_args = {
    "owner": "antigravity",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def serialize_mongo_doc(doc):
    """
    Recursively converts MongoDB BSON types (ObjectId, datetime, Decimal128)
    into standard JSON-serializable types.
    """
    if isinstance(doc, dict):
        new_doc = {}
        for k, v in doc.items():
            if k == "_id":
                new_doc["_id"] = str(v)
            else:
                new_doc[k] = serialize_mongo_doc(v)
        return new_doc
    elif isinstance(doc, list):
        return [serialize_mongo_doc(x) for x in doc]
    elif hasattr(doc, "isoformat"):  # DateTime/Timestamp objects
        return doc.isoformat()
    elif hasattr(doc, "__str__") and doc.__class__.__name__ in (
        "ObjectId",
        "Decimal128",
    ):
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


def resolve_uri_from_env(env_key: str):
    raw = os.environ.get(env_key)
    if not raw:
        return None

    raw = raw.strip()
    if raw.startswith("{") and raw.endswith("}"):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                if data.get("value"):
                    return data["value"]
                if data.get("isSecret"):
                    return None
        except json.JSONDecodeError:
            pass

    return raw


def get_mongo_client(mongo_conn_id):
    """Build a MongoDB client from the configured Airflow connection."""
    from pymongo import MongoClient
    # Prefer explicit AIRFLOW_CONN_<CONN_ID> environment variable if set
    env_key = f"AIRFLOW_CONN_{mongo_conn_id.upper()}"
    env_uri = resolve_uri_from_env(env_key)
    if env_uri:
        # Env var should contain a full MongoDB URI (mongodb:// or mongodb+srv://)
        return MongoClient(env_uri)

    # Try to get the URI from the Airflow Connection object (preserves +srv)
    conn = BaseHook.get_connection(mongo_conn_id)
    extra = getattr(conn, "extra_dejson", {}) or {}
    uri = extra.get("uri") or extra.get("connection_string")

    # Some Airflow installations expose the original URI via Connection.get_uri()
    try:
        conn_uri = conn.get_uri()
    except Exception:
        conn_uri = None

    if conn_uri and (conn_uri.startswith("mongodb://") or conn_uri.startswith("mongodb+srv://")):
        return MongoClient(conn_uri)

    if uri and (uri.startswith("mongodb://") or uri.startswith("mongodb+srv://")):
        return MongoClient(uri)

    # Fallback: construct a URI from host/login/port/schema if present.
    # Prefer Atlas SRV form when connecting to a mongodb.net cluster without a port.
    if conn.host:
        host = conn.host
        if conn.port:
            host = f"{host}:{conn.port}"
            scheme = "mongodb"
        elif ".mongodb.net" in conn.host:
            scheme = "mongodb+srv"
        else:
            scheme = "mongodb"

        auth = "" 
        if conn.login and conn.password:
            auth = f"{conn.login}:{conn.password}@"
        elif conn.login:
            auth = f"{conn.login}@"

        return MongoClient(f"{scheme}://{auth}{host}/{conn.schema or ''}")

    raise ValueError(f"Mongo connection '{mongo_conn_id}' is missing host/URI details")


def resolve_collection_plan(mongo_conn_id, mongo_db_name, configured_mappings):
    """Return the collection plan to process.

    If the repo has explicit schema mappings, use them. Otherwise, discover
    collection names from MongoDB and build a default mapping for each one.
    """
    if configured_mappings:
        return configured_mappings

    client = get_mongo_client(mongo_conn_id)
    db = client[mongo_db_name]
    discovered = {}

    for collection_name in db.list_collection_names():
        if collection_name.startswith("system."):
            continue

        safe_name = re.sub(r"[^A-Za-z0-9_]", "_", collection_name)
        discovered[collection_name] = {
            "table_name": f"RAW_{safe_name.upper()}",
            "view_name": f"V_{safe_name.upper()}",
            "fields": {},
        }

    return discovered


@dag(
    dag_id="mongodb_to_snowflake",
    default_args=default_args,
    description="ELT pipeline to migrate MongoDB data to Snowflake",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mongodb", "snowflake", "elt"],
    params={
        "mongo_conn_id": "mongo_conn_id",
        "snowflake_conn_id": "snowflake_conn_id",
        "snowflake_database": "ORAZOX_DB",
        "snowflake_schema": "PUBLIC",
        "snowflake_warehouse": "COMPUTE_WH",
        "mongo_db_name": MONGO_DATABASE,
        "batch_size": 5000,
    },
)
def mongodb_to_snowflake_etl():
    @task(task_id="initialize_snowflake_resources")
    def init_snowflake(**context):
        """
        Initializes target Database, Schema, and Internal Stage in Snowflake.
        """
        params = context["params"]
        sf_conn = params["snowflake_conn_id"]
        database = params["snowflake_database"]
        schema = params["snowflake_schema"]
        warehouse = params["snowflake_warehouse"]  # noqa: F841

        sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)

        setup_queries = [
            f"CREATE DATABASE IF NOT EXISTS {database};",
            f"CREATE SCHEMA IF NOT EXISTS {database}.{schema};",
            f"CREATE STAGE IF NOT EXISTS {database}.{schema}.mongodb_stage;",
        ]

        logger.info("Setting up database, schema, and internal stage...")
        for query in setup_queries:
            sf_hook.run(query, autocommit=True)

    @task(task_id="discover_collections")
    def discover_collections(**context):
        """Discover collections at runtime and return a list of mapping dicts.

        Returns list of {collection, table_name, view_name, fields}.
        """
        params = context["params"]
        mongo_conn = params["mongo_conn_id"]
        mongo_db = params["mongo_db_name"]

        if SCHEMA_MAPPINGS:
            plan = []
            for collection, mapping in SCHEMA_MAPPINGS.items():
                plan.append(
                    {
                        "collection": collection,
                        "table_name": mapping["table_name"],
                        "view_name": mapping["view_name"],
                        "fields": mapping.get("fields", {}),
                    }
                )
            return plan

        client = get_mongo_client(mongo_conn)
        db = client[mongo_db]
        plan = []
        for collection_name in db.list_collection_names():
            if collection_name.startswith("system."):
                continue

            safe_name = re.sub(r"[^A-Za-z0-9_]", "_", collection_name)
            plan.append(
                {
                    "collection": collection_name,
                    "table_name": f"RAW_{safe_name.upper()}",
                    "view_name": f"V_{safe_name.upper()}",
                    "fields": {},
                }
            )

        return plan


    @task(task_id="elt_collection", multiple_outputs=True)
    def run_collection_elt(collection: str, table: str, view: str, fields_map: dict, **context):
        params = context["params"]
        mongo_conn = params["mongo_conn_id"]
        sf_conn = params["snowflake_conn_id"]
        database = params["snowflake_database"]
        schema = params["snowflake_schema"]
        mongo_db = params["mongo_db_name"]
        batch_size = int(params["batch_size"])

        # 1. Extract from MongoDB and write to local JSONL
        logger.info(
            f"Connecting to MongoDB and fetching from collection '{collection}'..."
        )
        client = get_mongo_client(mongo_conn)
        db = client[mongo_db]
        cursor = db[collection].find()

        temp_dir = tempfile.gettempdir()
        # IMPORTANT: do NOT use run_id here — it contains ':' and '+' which
        # are not valid in an unquoted Snowflake stage URI and cause
        # COPY INTO to silently match zero files when ON_ERROR=CONTINUE.
        safe_run_token = context["ts_nodash"]
        filename = f"mongo_export_{collection}_{safe_run_token}.json"
        local_filepath = os.path.join(temp_dir, filename)

        record_count = 0
        logger.info(f"Writing records to temporary local file {local_filepath}...")
        with open(local_filepath, "w", encoding="utf-8") as f:
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
            logger.info(
                f"No records found in collection '{collection}'. Skipping Snowflake load."
            )
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
        logger.info(
            f"Uploading file to Snowflake stage: @{database}.{schema}.mongodb_stage..."
        )
        conn = sf_hook.get_conn()
        sf_cursor = conn.cursor()
        try:
            # PUT requires clean UNIX style path separators
            clean_path = local_filepath.replace(os.sep, "/")
            put_sql = f"PUT 'file://{clean_path}' @{database}.{schema}.mongodb_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
            sf_cursor.execute(put_sql)
        finally:
            sf_cursor.close()
            conn.close()

        # 5. COPY INTO Temp Table
        logger.info(f"Loading staged files into temp table {temp_table}...")
        staged_file = f"{filename}.gz"
        copy_sql = f"""
        COPY INTO {database}.{schema}.{temp_table} (_MONGO_ID, DATA, INGESTED_AT)
        FROM (
            SELECT
                $1:_id::VARCHAR,
                $1,
                CURRENT_TIMESTAMP()
            FROM @{database}.{schema}.mongodb_stage
        )
        FILES = ('{staged_file}')
        FILE_FORMAT = (TYPE = 'JSON')
        ON_ERROR = 'ABORT_STATEMENT';
        """
        copy_results = sf_hook.run(
            copy_sql, autocommit=True, handler=lambda c: c.fetchall()
        )
        logger.info(f"COPY INTO result for {staged_file}: {copy_results}")

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
            {", ".join(select_clauses)}
        FROM {database}.{schema}.{table};
        """
        sf_hook.run(view_ddl, autocommit=True)

        # Cleanup Local Temp File
        if os.path.exists(local_filepath):
            os.remove(local_filepath)
            logger.info(f"Removed local temporary file {local_filepath}")

        return {"records_loaded": record_count, "status": "SUCCESS"}

    init_db = init_snowflake()

    discovered = discover_collections()

    # Map the per-collection task across discovered collection plan
    # Use XCom returned list from discover_collections; Airflow mapping will
    # expand with the lists below.
    mapped = run_collection_elt.expand(
        collection=discovered.map(lambda x: x["collection"]),
        table=discovered.map(lambda x: x["table_name"]),
        view=discovered.map(lambda x: x["view_name"]),
        fields_map=discovered.map(lambda x: x["fields"]),
    )

    init_db >> discovered >> mapped


# Define the DAG instantiation
mongodb_to_snowflake_dag = mongodb_to_snowflake_etl()
