import pendulum  # datetime 확장 라이브러리
import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

"""
DAG(
dag_id: dag의 id. 일반적으로 파일 이름과 통일시한다.
schedule: 실행 일시. 문법은 cron을 따른다. 아래의 경우 "매일 0시 0분"에 실행.
start_date: dag 시작 시간
catchup: start_date가 현재 시점보다 과거일 경우, 누락된 부분을 실행할지 여부. 이때 start_date부터 schedule에 따라 N번 실행되는 것이 아닌 한꺼번에 한 번만 실행된다. 
dagrun_timeout: dag 실행 Timeout. 아래의 경우 60분.
tags: 현재 DAG의 tag들. 나중에 tags들을 통해 묶어서 DAG를 확인할 수 있다.
params: DAG 내부에서 사용할 파라미터들.
)
"""


with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=[],
    params={}

) as dag:
    t1 = EmptyOperator(task_id="Empty_t1")
    t2 = EmptyOperator(task_id="Empty_t2")
    t3 = EmptyOperator(task_id="Empty_t3")
    t4 = EmptyOperator(task_id="Empty_t4")
    t5 = EmptyOperator(task_id="Empty_t5")
    t6 = EmptyOperator(task_id="Empty_t6")
    t7 = EmptyOperator(task_id="Empty_t7")
    final_task = BashOperator(
        task_id="final_task",
        bash_command="echo $HOSTNAME; echo good",
        # $HOSTNAME을 출력하면 Worker Docker Container의 REPOSITORY_ID가 출력된다.(Worker 컨테이너가 받아 수행하기 때문)
    )

    t1 >> [t2, t3] >> t4
    t5 >> [t4, t7]
    [t4, t5] >> t6
    [t6, t7] >> final_task
