from dagster import ScheduleDefinition
from Pac_company_dagster.defs.jobs.sales_jobs import sales_job

sales_daily_schedule = ScheduleDefinition(
    job=sales_job,
    cron_schedule=" 35 8 * * *",
    execution_timezone="Asia/Jakarta",  
)


# ┌ minute (0)
# │ ┌ hour (8)
# │ │ ┌ day of month (*)
# │ │ │ ┌ month (*)
# │ │ │ │ ┌ day of week (*)
# 0 8 * * *
