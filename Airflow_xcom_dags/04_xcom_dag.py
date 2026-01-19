import pendulum
from airflow.sdk import DAG, get_current_context, task

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

with DAG(
    dag_id = 'xcom04_dag',
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20251223'],
    default_args = default_args,
    catchup=False
):  
    @task(task_id="first", do_xcom_push=True, multiple_outputs=True)
    def first_func(args):
        join_list = ' '.join(args)
        
        return {"key1": join_list}
    
    args_list = ['FLOWER', 'AIRFLOW', 'BIGQUERY']
    
    message = first_func(args_list)