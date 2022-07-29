from hacker_news.assets import items, comments, stories
from hacker_news.snowflake import clone_prod
from dagster import repository, with_resources
from dagster_snowflake import build_snowflake_io_manager, snowflake_resource
from dagster_snowflake_pandas import SnowflakePandasTypeHandler
import os

snowflake_io_manager = build_snowflake_io_manager([SnowflakePandasTypeHandler()])


@repository
def repo():
    snowflake_config = {
        "account": {"env": "SNOWFLAKE_ACCOUNT"},
        "user": {"env": "SNOWFLAKE_USER"},
        "password": {"env": "SNOWFLAKE_PASSWORD"},
        "database": "SANDBOX"
        "warehouse": "ELEMENTL"
    }
    resource_defs = {
        "branch": {
            "snowflake_io_manager": snowflake_io_manager.configured(
                {
                    **snowflake_config,
                    "schema": f"PRODUCTION_CLONE_{os.getenv('DAGSTER_CLOUD_PULL_REQUEST_ID')}",
                }
            ),
            "snowflake": snowflake_resource.configured(
            {
                **snowflake_config,
                "schema": f"PRODUCTION_CLONE_{os.getenv('DAGSTER_CLOUD_PULL_REQUEST_ID')}",
            }
        ),
        },
        "production": {
            "snowflake_io_manager": snowflake_io_manager.configured(
                {
                    **snowflake_config,
                    "schema": "PRODUCTION"
                }
            ),
            "snowflake": snowflake_resource.configured({**snowflake_config, "schema": "PRODUCTION"}),

        },
    }

    def get_current_env():
        is_branch_depl = os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT")
        assert is_branch_depl != None  # env var must be set
        return "branch" if is_branch_depl else "prod"

    return [
        with_resources([items, comments, stories], resource_defs=resource_defs[get_current_env()]),
        clone_prod.to_job(resource_defs=resource_defs[get_current_env()]),

    ]
