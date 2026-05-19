from airflow.sdk import dag, task
from datetime import datetime

@dag(dag_id="xcom_auto")
def xcom_auto():

    @task.python
    def extract():
        print("Extracting data...")
        return {"name": "Airflow", "version": "2.0"}
    
    @task.python
    def transform(data: dict):
        print("Transforming data...")
        data["version"] = "2.1"
        return data
    
    @task.python
    def load(data: dict):
        print("Loading data...")
        print(f"Data: {data}")

    data = extract()
    transformed_data = transform(data)
    load_data = load(transformed_data)


#run the dags
xcom_auto()