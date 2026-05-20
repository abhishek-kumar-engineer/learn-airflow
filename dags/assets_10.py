from airflow.sdk import dag, task, asset
import os


@asset(
    schedule="@daily",
    uri='/opt/airflow/logs/data/raw_data_extract.txt',
    name='fetch_data'
)
def fetch_data(self):

    # Ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, 'w') as f:
        f.write(f"Data extracted at ...")

    print(f"Data fetched and stored at {self.uri}")

