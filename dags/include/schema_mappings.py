# Schema mappings for the MongoDB -> Snowflake pipeline.
#
# MONGO_DATABASE: source MongoDB database name.
# SCHEMA_MAPPINGS: dict keyed by MongoDB collection name with:
#   - table_name: Snowflake RAW table name (VARIANT-based)
#   - view_name:  Snowflake relational VIEW name on top of the RAW table
#   - fields:     {mongo_field_name: snowflake_sql_type} used to build the view
#
# TODO: Replace these placeholders with your real collections and field maps.

MONGO_DATABASE = "supplier_db"

SCHEMA_MAPPINGS = {
    # Example placeholder mapping; replace with real collections:
    # "suppliers": {
    #     "table_name": "RAW_SUPPLIERS",
    #     "view_name": "V_SUPPLIERS",
    #     "fields": {
    #         "name": "VARCHAR",
    #         "country": "VARCHAR",
    #         "createdAt": "TIMESTAMP_TZ",
    #         "rating": "FLOAT",
    #     },
    # },
}
