from airflow.sdk import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.timetables.trigger import DeltaTriggerTimetable
from pendulum import duration, datetime

@dag(
    dag_id="all_scheduler",
    start_date=datetime(year=2026, month=1, day=1, tz="UTC"),
    # schedule=CronTriggerTimetable("0 16 * * *", timezone='UTC'),  # Runs daily at midnight
    schedule=DeltaTriggerTimetable(duration(days=5)),  # Runs every 5 minutes
    catchup=False
)
def all_scheduler():
    @task
    def cron_task():
        print("This task runs on a cron schedule.")

    @task
    def delta_task():
        print("This task runs on a delta schedule.")

    cron = cron_task()
    delta = delta_task()

    cron >> delta  # Define task dependencies

all_scheduler()
