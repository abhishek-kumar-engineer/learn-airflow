from airflow.sdk import dag, task


@dag()
def dag_versioning():
    print('this is my first dag')

    @task.python
    def first_task():
        print('this is my first task')

    @task.python
    def second_task():
        print('this is my second task')

    @task.python
    def third_task():
        print('this is my third task')
    
    @task.python
    def version_task():
        print('versioning of task')

    first = first_task()
    second = second_task()
    third = third_task()
    version = version_task()

    first >>  second >> third >> version
    
dag_versioning()