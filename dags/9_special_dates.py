from airflow.sdk import dag, task
from airflow.timetables.events import EventsTimetable
from pendulum import datetime


special_dates = EventsTimetable(
    events=[datetime(year=2026, month=12, day=18, tz="UTC"), datetime(year=2026, month=7, day=7, tz="UTC")])

@dag(schedule=special_dates, start_date=datetime(year=2026, month=1, day=1, tz="UTC"), catchup=True)
def special_dates_dag():
    @task
    def print_execution_date(execution_date=None):
        print(f"Execution date: {execution_date}")

    print_execution_date()

special_dates_dag()