from dagster import ScheduleDefinition
from ..jobs import rawcooked_start_jobs

rawcooked_schedule = ScheduleDefinition(
    job=rawcooked_start_jobs,
    cron_schedule = "*/15 * * * *",
)