from airflow.sdk import dag, task


@dag()
def first_dag():
    print('this is my first dag')

    @task()
    def first_task():
        print('this is my first task')

    @task()
    def second_task():
        print('this is my second task')

    @task()
    def third_task():
        print('this is my third task')
    

    first = first_task()
    second = second_task()
    third = third_task()

    first >>  second >> third

first_dag()