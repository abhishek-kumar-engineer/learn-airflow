from airflow.sdk import dag, task

@dag(dag_id='parallel_task')
def parallel_task():
    
    @task.python
    def extract_data(**kwargs):
        print('Extracting data...  ')
        ti = kwargs['ti']
        extracted_data = {'extracted_api_data': [1,2,3], 'extracted_db_data': [4,5,6], 'extracted_file_data': [7,8,9]}
        ti.xcom_push(key='return_value', value=extracted_data)
    
    @task.python
    def transform_api_data(**kwargs):
        print('Transforming API data...  ')
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='return_value', task_ids='extract_data')
        api_data = extracted_data['extracted_api_data']
        transformed_api_data = [x * 2 for x in api_data]
        ti.xcom_push(key='return_value', value=transformed_api_data)
        print(f'Transformed API data: {transformed_api_data}')
    
    @task.python
    def transform_db_data(**kwargs):
        print('Transforming DB data...  ')
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='return_value', task_ids='extract_data')
        db_data = extracted_data['extracted_db_data']
        transformed_db_data = [x * 3 for x in db_data]
        ti.xcom_push(key='return_value', value=transformed_db_data)
        print(f'Transformed DB data: {transformed_db_data}')
    
    @task.python
    def transform_file_data(**kwargs):
        print('Transforming file data...  ')
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='return_value', task_ids='extract_data')
        file_data = extracted_data['extracted_file_data']
        transformed_file_data = [x * 4 for x in file_data]
        ti.xcom_push(key='return_value', value=transformed_file_data)
        print(f'Transformed file data: {transformed_file_data}')

    @task.bash
    def load_data(**kwargs):
        print('Loading data...  ')
        ti = kwargs['ti']
        transformed_api_data = ti.xcom_pull(key='return_value', task_ids='transform_api_data')
        transformed_db_data = ti.xcom_pull(key='return_value', task_ids='transform_db_data')
        transformed_file_data = ti.xcom_pull(key='return_value', task_ids='transform_file_data')
        return f'echo Loaded data: {transformed_api_data}, {transformed_db_data}, {transformed_file_data}'

    extracted_data = extract_data()
    transform_api_data = transform_api_data()
    transform_db_data = transform_db_data()
    transform_file_data = transform_file_data()
    load_data = load_data()

    extracted_data >> [transform_api_data, transform_db_data, transform_file_data] >> load_data

parallel_task()