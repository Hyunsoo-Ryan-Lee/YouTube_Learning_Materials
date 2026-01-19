import pendulum
from airflow.sdk import DAG, get_current_context, task

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

with DAG(
    dag_id = 'xcom03_dag',
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20251223'],
    default_args = default_args,
    catchup=False
):  
    @task(task_id="first")
    def first_func(args):
        join_list = ' '.join(args)
        
        return join_list
        
    @task(task_id='second')
    def second_func(message):
        # 작업 수행
        changed_list = '!' + message + '!' # '!FLOWER AIRFLOW BIGQUERY!'
        
        return changed_list
    
    @task(task_id='third')
    def third_func(message):
        
        result = message.split(' ') # ['!FLOWER', 'AIRFLOW', 'BIGQUERY!']
        print("작업 결과 : ", result)
        
        return result
    
    args_list = ['FLOWER', 'AIRFLOW', 'BIGQUERY']
    
    message = first_func(args_list)
    
    message2 = second_func(message)
    
    third_func(message2)