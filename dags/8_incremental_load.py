from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(dag_id="incremental_load",
     start_date=datetime(year=2026, month=5, day=15, tz="UTC"),
     end_date=datetime(year=2026, month=5, day=19, tz="UTC"),
     schedule=CronDataIntervalTimetable("@daily", timezone='UTC'),  # Runs daily at 00:00 UTC
     catchup=True)
def incremental_load():

    @task.python
    def incremental_fetch_data(**kwargs):
        start_date = kwargs['data_interval_start']
        end_date = kwargs['data_interval_end']
        print(f"Fetching data from {start_date} to {end_date}")
    
    @task.bash
    def incremental_process_data():
        return 'echo "Processing incremental data from {{ data_interval_start }} to {{ data_interval_end }}"'
    
    fetch_data = incremental_fetch_data()
    process_data = incremental_process_data()

    fetch_data >> process_data

incremental_load()
