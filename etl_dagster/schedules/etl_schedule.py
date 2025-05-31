from dagster import ScheduleDefinition
from datetime import time

etl_schedule = ScheduleDefinition(
    job_name="assets_job",
    cron_schedule="0 6 * * *",  # tous les jours à 6h du matin
    execution_timezone="Europe/Paris",
    run_config={},  # pas de config dynamique
)
