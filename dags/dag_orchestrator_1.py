from airflow.sdk import dag, task

@dag(dag_id="first_dag_orchestrator")
def first_dag_orchestrator():
    @task(task_id="first_task")
    def first_task():
        print("This is the first task")

    @task(task_id="second_task")
    def second_task():
        print("This is the second task")

    first_task() >> second_task()

first_dag_orchestrator()