from dagster import WeeklyPartitionsDefinition
from ..assets import constants

start_date = constants.START_DATE
end_date = constants.END_DATE

weekly_partition = WeeklyPartitionsDefinition(
    start_date=start_date,
    end_date=end_date
)