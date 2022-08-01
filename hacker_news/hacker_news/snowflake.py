from dagster import op, graph, In, Nothing
import os

@op(required_resource_keys={"snowflake"})
def drop_database_clone(context):
    context.log.info(f"SNOWFLAKE_ACCOUNT: {os.getenv('SNOWFLAKE_ACCOUNT')}")
    context.log.info(f"SNOWFLAKE_USER: {os.getenv('SNOWFLAKE_USER')}")
    context.log.info(f"SNOWFLAKE_PASSWORD: {os.getenv('SNOWFLAKE_PASSWORD')}")

    query = f"DROP SCHEMA IF EXISTS BOLLINGER.PRODUCTION_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']}"
    context.log.info(f"QUERY {query}")
    context.resources.snowflake.execute_query(
        query
    )


@op(required_resource_keys={"snowflake"}, ins={"start": In(Nothing)})
def clone_production_database(context):
    context.resources.snowflake.execute_query(
        f"CREATE SCHEMA BOLLINGER.PRODUCTION_CLONE_{os.environ['DAGSTER_CLOUD_PULL_REQUEST_ID']} CLONE \"PRODUCTION\""
    )


@graph
def clone_prod():
    clone_production_database(start=drop_database_clone())
