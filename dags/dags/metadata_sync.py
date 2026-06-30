import os
import sys
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# Ensure the include directory is in the import path
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../include"))
)
try:
    from schema_mappings import SCHEMA_MAPPINGS, MONGO_DATABASE
except ImportError:
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


@dag(
    dag_id="metadata_sync",
    default_args=default_args,
    description="Logs and audits pipeline metadata and sync stats in Snowflake",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["metadata", "audit", "snowflake"],
    params={
        "mongo_conn_id": "mongo_conn_id",
        "snowflake_conn_id": "snowflake_conn_id",
        "snowflake_database": "ORAZOX_DB",
        "snowflake_schema": "PUBLIC",
        "mongo_db_name": MONGO_DATABASE,
    },
)
def metadata_sync_dag():
    def get_mongo_client(mongo_conn_id):
        from pymongo import MongoClient

        conn = BaseHook.get_connection(mongo_conn_id)
        extra = conn.extra_dejson or {}

        if conn.host:
            host = conn.host
            if conn.port:
                host = f"{host}:{conn.port}"
            if conn.login and conn.password:
                uri = f"mongodb://{conn.login}:{conn.password}@{host}"
            elif conn.login:
                uri = f"mongodb://{conn.login}@{host}"
            else:
                uri = f"mongodb://{host}"
        else:
            uri = extra.get("uri") or extra.get("connection_string")

        if not uri:
            raise ValueError(
                f"Mongo connection '{mongo_conn_id}' is missing host/URI details"
            )

        return MongoClient(uri)

    @task(task_id="setup_metadata_table")
    def setup_metadata_table(**context):
        """
        Creates the persistent METADATA_SYNC_LOG audit table in Snowflake if it doesn't exist.
        """
        params = context["params"]
        sf_conn = params["snowflake_conn_id"]
        database = params["snowflake_database"]
        schema = params["snowflake_schema"]

        sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.METADATA_SYNC_LOG (
            SYNC_ID VARCHAR(64) DEFAULT UUID_STRING(),
            RUN_ID VARCHAR(256),
            COLLECTION_NAME VARCHAR(128),
            SF_TABLE_NAME VARCHAR(128),
            SF_VIEW_NAME VARCHAR(128),
            MONGO_COUNT NUMBER,
            SF_COUNT NUMBER,
            SYNCED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
            STATUS VARCHAR(32)
        );
        """
        sf_hook.run(create_table_sql, autocommit=True)
        logger.info("Metadata sync log table setup completed.")

    @task(task_id="sync_audit_stats")
    def sync_audit_stats(**context):
        """
        Calculates counts from MongoDB and Snowflake, then writes them to the audit log table.
        """
        params = context["params"]
        mongo_conn = params["mongo_conn_id"]
        sf_conn = params["snowflake_conn_id"]
        database = params["snowflake_database"]
        schema = params["snowflake_schema"]
        mongo_db = params["mongo_db_name"]
        run_id = context["run_id"]

        mongo_hook = get_mongo_client(mongo_conn)
        sf_hook = SnowflakeHook(snowflake_conn_id=sf_conn)
        client = mongo_hook
        db = client[mongo_db]

        insert_values = []

        logger.info("Gathering metrics for sync audit log...")
        for col, mapping in SCHEMA_MAPPINGS.items():
            table = mapping["table_name"]
            view = mapping["view_name"]

            # MongoDB Count
            try:
                mongo_count = db[col].count_documents({})
            except Exception as e:
                logger.error(
                    f"Error fetching count for Mongo collection '{col}': {str(e)}"
                )
                mongo_count = -1

            # Snowflake Count
            try:
                # Check if table exists
                table_check_sql = f"""
                SELECT COUNT(*)
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_NAME = '{table}'
                AND TABLE_SCHEMA = '{schema}';
                """
                table_exists = sf_hook.get_first(table_check_sql)[0] > 0

                if table_exists:
                    sf_count_sql = f"SELECT COUNT(*) FROM {database}.{schema}.{table}"
                    sf_count = sf_hook.get_first(sf_count_sql)[0]
                else:
                    sf_count = -1
            except Exception as e:
                logger.error(
                    f"Error fetching count for Snowflake table '{table}': {str(e)}"
                )
                sf_count = -1

            # Status determination
            if mongo_count == -1 or sf_count == -1:
                status = "ERROR"
            elif mongo_count != sf_count:
                status = "MISMATCH"
            else:
                status = "SYNCED"

            # Escaping strings for SQL insert
            escaped_run_id = run_id.replace("'", "''")
            insert_values.append(
                f"('{escaped_run_id}', '{col}', '{table}', '{view}', {mongo_count}, {sf_count}, '{status}')"
            )

        if insert_values:
            insert_sql = f"""
            INSERT INTO {database}.{schema}.METADATA_SYNC_LOG
            (RUN_ID, COLLECTION_NAME, SF_TABLE_NAME, SF_VIEW_NAME, MONGO_COUNT, SF_COUNT, STATUS)
            VALUES {", ".join(insert_values)};
            """
            sf_hook.run(insert_sql, autocommit=True)
            logger.info(
                f"Successfully recorded metadata sync logs for {len(insert_values)} collections."
            )
        else:
            logger.info("No collection mappings found to audit.")

    setup_table = setup_metadata_table()
    audit_stats = sync_audit_stats()

    setup_table >> audit_stats


# Instantiation
metadata_sync_dag = metadata_sync_dag()
