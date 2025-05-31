from dagster_dbt import DbtCliResource, dbt_assets
from dagster import AssetExecutionContext

@dbt_assets(manifest="dbt/joconde/target/manifest.json")
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()