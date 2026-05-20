from dag_orchestrator_1 import first_dag_orchestrator
from dag_orchestrator_2 import second_dag_orchestrator
from airflow.sdk import dag, task
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator

dag(dag_id = 'parent_dag_of_1_2_orchestrator')
def parent_dag_of_1_2_orchestrator():

    @task(task_id="first_task")
    def first_task():
        first_dag_orchestrator()
    
    @task(task_id="second_task")
    def second_task():
        second_dag_orchestrator()
    
    first_task() >> second_task()

parent_dag_of_1_2_orchestrator()
