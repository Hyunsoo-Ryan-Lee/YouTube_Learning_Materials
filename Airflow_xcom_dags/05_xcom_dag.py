import pendulum
from airflow.sdk import DAG, get_current_context, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator 

def make_dirname():
    context = get_current_context()
    ti = context["ti"]
    dir_path = f"/opt/airflow/plugins/report_20260117"
    ti.xcom_push(key="dir_path", value=dir_path)

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

with DAG(
    dag_id = 'xcom05_dag',
    start_date=pendulum.datetime(2025, 6, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *", # cron 표현식
    tags = ['20251223'],
    default_args = default_args,
    catchup=False
):  

    t1 = PythonOperator(
        task_id="make_dirname",
        python_callable=make_dirname,
    )

    t2 = BashOperator(
        task_id="make_dir",
        bash_command="mkdir -p {{ ti.xcom_pull(task_ids='make_dirname', key='dir_path') }}"
    )

    t1 >> t2