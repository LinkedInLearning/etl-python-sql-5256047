from dagster import Definitions
from assets.extract import donnees_brutes
from assets.transform import donnees_transformees
from assets.load import chargement_sqlserver
from dagster_dbt import DbtCliResource
from assets.dbt_assets import dbt_models

defs = Definitions(
    assets=[
        donnees_brutes,
        donnees_transformees,
        chargement_sqlserver,
        dbt_models
    ],
    resources={
        "dbt": DbtCliResource(project_dir="dbt/joconde")
    }
)