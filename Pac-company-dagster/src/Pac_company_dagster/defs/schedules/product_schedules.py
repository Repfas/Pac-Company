from dagster import ScheduleDefinition
from Pac_company_dagster.defs.jobs.product_jobs import product_job

product_daily_schedule = ScheduleDefinition(
    job=product_job,
    cron_schedule=" 37 8 * * *",
    execution_timezone="Asia/Jakarta",  
)


# ┌ minute (0)
# │ ┌ hour (8)
# │ │ ┌ day of month (*)
# │ │ │ ┌ month (*)
# │ │ │ │ ┌ day of week (*)
# 0 8 * * *
