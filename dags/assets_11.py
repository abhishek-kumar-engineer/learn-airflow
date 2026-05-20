from airflow.sdk import dag, task, asset
import os
from assets_10 import fetch_data 

@asset(
    schedule=[fetch_data],
    uri='/opt/airflow/logs/data/processed_data_transform.txt',
    name='transform_data'
)
def transform_data(self):

    # Ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    with open(self.uri, 'w') as f:
        f.write(f"Data transformed at ...")

    print(f"Data transformed and stored at {self.uri}")

