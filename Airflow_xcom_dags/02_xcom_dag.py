import pendulum
from pprint import pprint
from airflow.sdk import DAG, get_current_context
from airflow.providers.standard.operators.python import PythonOperator

"""
PythonOperator
- python 함수를 airflow에서 실행할 수 있도록 해주는 오퍼레이터
"""

def push_to_xcom():
    message = "사과"
    context = get_current_context()
    pprint(context)
    ti = context['ti']
    
    ti.xcom_push(
        key='message',
        value=message
    )

    return message # 'return_value'라는 키로 저장됨 

def pull_from_xcom():
    context = get_current_context()
    ti = context['ti']
    
    xcom_value = ti.xcom_pull(
        task_ids='py1',
        key='message'
    )
    
    print("py1에서 전달받은 결과 : ", xcom_value)


default_args = dict(
    owner = 'hyunsoo',
    email = ['hyunsoo@airflow.com'],
    email_on_failure = False,
    retries = 3
)

with DAG(
    dag_id="xcom02_dag",
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['202501223'],
    default_args = default_args,
    catchup=False
):
    py1 = PythonOperator(
        task_id = 'py1',
        python_callable=push_to_xcom,
    )

    py2 = PythonOperator(
        task_id = 'py2',
        python_callable=pull_from_xcom,
    )
    
py1 >> py2