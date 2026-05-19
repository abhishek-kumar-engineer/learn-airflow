from airflow.sdk import dag, task


@dag()
def operators_dag():
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
    
    @task.bash
    def bash_operator_modern():
        return 'echo https://airflow.apach.org/'
    

    first = first_task()
    second = second_task()
    third = third_task()
    bash_operator = bash_operator_modern()

    first >>  second >> third >> bash_operator

operators_dag()