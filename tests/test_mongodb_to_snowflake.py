import importlib.util
import sys
import types
from pathlib import Path


class DummyDecorator:
    def __call__(self, *args, **kwargs):
        def decorator(func):
            def wrapper(*wrapper_args, **wrapper_kwargs):
                return None

            return wrapper

        return decorator


fake_airflow = types.ModuleType("airflow")
fake_decorators = types.ModuleType("airflow.decorators")
fake_decorators.dag = DummyDecorator()
fake_decorators.task = DummyDecorator()
sys.modules.setdefault("airflow", fake_airflow)
sys.modules.setdefault("airflow.decorators", fake_decorators)

fake_utils = types.ModuleType("airflow.utils")
fake_task_group = types.ModuleType("airflow.utils.task_group")
fake_task_group.TaskGroup = object
sys.modules.setdefault("airflow.utils", fake_utils)
sys.modules.setdefault("airflow.utils.task_group", fake_task_group)

fake_hooks = types.ModuleType("airflow.hooks")
fake_base = types.ModuleType("airflow.hooks.base")


class BaseHook:
    @staticmethod
    def get_connection(_conn_id):
        raise RuntimeError("Not implemented")


fake_base.BaseHook = BaseHook
sys.modules.setdefault("airflow.hooks", fake_hooks)
sys.modules.setdefault("airflow.hooks.base", fake_base)

fake_providers = types.ModuleType("airflow.providers")
fake_snowflake = types.ModuleType("airflow.providers.snowflake")
fake_snowflake_hooks = types.ModuleType("airflow.providers.snowflake.hooks")
fake_snowflake_module = types.ModuleType("airflow.providers.snowflake.hooks.snowflake")


class SnowflakeHook:
    def __init__(self, *args, **kwargs):
        pass


fake_snowflake_module.SnowflakeHook = SnowflakeHook
sys.modules.setdefault("airflow.providers", fake_providers)
sys.modules.setdefault("airflow.providers.snowflake", fake_snowflake)
sys.modules.setdefault("airflow.providers.snowflake.hooks", fake_snowflake_hooks)
sys.modules.setdefault("airflow.providers.snowflake.hooks.snowflake", fake_snowflake_module)


MODULE_PATH = Path(__file__).resolve().parents[1] / "dags" / "dags" / "mongodb_to_snowflake.py"


spec = importlib.util.spec_from_file_location("mongodb_to_snowflake", MODULE_PATH)
mongodb_to_snowflake = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mongodb_to_snowflake)


class FakeMongoClient:
    def __init__(self, collections):
        self._collections = collections

    def __getitem__(self, db_name):
        return FakeDatabase(self._collections)


class FakeDatabase:
    def __init__(self, collections):
        self._collections = collections

    def list_collection_names(self):
        return self._collections


def test_resolve_collection_plan_falls_back_to_discovered_collections(monkeypatch):
    fake_client = FakeMongoClient(["products", "suppliers", "system.indexes"])

    monkeypatch.setattr(mongodb_to_snowflake, "get_mongo_client", lambda conn_id: fake_client)

    plan = mongodb_to_snowflake.resolve_collection_plan(
        mongo_conn_id="mongo_conn_id",
        mongo_db_name="supplier_db",
        configured_mappings={},
    )

    assert plan["products"]["table_name"] == "RAW_PRODUCTS"
    assert plan["products"]["view_name"] == "V_PRODUCTS"
    assert plan["suppliers"]["table_name"] == "RAW_SUPPLIERS"
    assert "system.indexes" not in plan
