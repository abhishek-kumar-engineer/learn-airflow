from dag_orchestrator_1 import first_dag_orchestrator
from dag_orchestrator_2 import second_dag_orchestrator
from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(dag_id = 'parent_dag_of_1_2_orchestrator')
def parent_dag_of_1_2_orchestrator():

    trigger_first = TriggerDagRunOperator(
        task_id="trigger_first_dag",
        trigger_dag_id="first_dag_orchestrator",   # ✅ the dag_id string, not the function
        wait_for_completion=True,                  # ✅ waits before moving to next
        poke_interval=10,
    )

    trigger_second = TriggerDagRunOperator(
        task_id="trigger_second_dag",
        trigger_dag_id="second_dag_orchestrator",  # ✅ the dag_id string
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_first >> trigger_second  # ✅ run sequentially

parent_dag_of_1_2_orchestrator()
