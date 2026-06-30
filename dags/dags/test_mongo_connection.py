import os
import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover - runtime only
    MongoClient = None

logger = logging.getLogger(__name__)


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


@dag(
    dag_id="test_mongo_connection",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 0},
)
def test_mongo_connection():
    @task()
    def check_connection():
        """Attempt to connect to MongoDB using AIRFLOW_CONN_MONGO_CONN_ID or the Airflow connection.

        This task is intended to be deployed to Astronomer and run once to verify
        network and credential configuration from the runtime environment.
        """
        uri = resolve_uri_from_env("AIRFLOW_CONN_MONGO_CONN_ID")
        if uri:
            logger.info("Using Mongo URI from env var")

        if not uri:
            try:
                conn = BaseHook.get_connection("mongo_conn_id")
                extras = getattr(conn, "extra_dejson", {}) or {}
                uri = extras.get("uri") or extras.get("connection_string")
                if not uri:
                    logger.info("Using Mongo URI from Airflow connection fallback")
                    host = conn.host
                    port = conn.port
                    login = conn.login
                    password = conn.password
                    schema = conn.schema or ""
                    if host:
                        if port:
                            host = f"{host}:{port}"
                            scheme = "mongodb"
                        elif ".mongodb.net" in host:
                            scheme = "mongodb+srv"
                        else:
                            scheme = "mongodb"
                        if login and password:
                            uri = f"{scheme}://{login}:{password}@{host}/{schema}"
                        elif login:
                            uri = f"{scheme}://{login}@{host}/{schema}"
                        else:
                            uri = f"{scheme}://{host}/{schema}"
            except Exception as e:
                logger.exception("Failed to read Airflow connection: %s", e)
                raise

        if not uri:
            raise RuntimeError("No MongoDB connection URI available (env or Airflow connection)")

        logger.info("Using Mongo URI: %s", uri if len(uri) < 200 else uri[:80] + "...[redacted]")

        if MongoClient is None:
            raise RuntimeError("pymongo not available in this environment")

        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=10000)
            names = client.list_database_names()
            logger.info("Mongo reachable — databases: %s", names)
            return {"ok": True, "databases": names}
        except Exception as e:
            logger.exception("Mongo connectivity check failed: %s", e)
            raise

    check_connection()


test_mongo_connection_dag = test_mongo_connection()
