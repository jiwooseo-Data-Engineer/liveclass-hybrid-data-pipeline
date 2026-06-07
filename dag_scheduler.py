from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# 1. 기본 스케줄 및 재시도 정책(Retry Policy) 설정
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False, # 과거 태스크가 실패했어도 오늘 배치는 독립적으로 실행
    'start_date': datetime(2026, 1, 1), # 2026년 1월 1일부터 과거 데이터 배치 가동 시작 가능하도록 세팅
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2, # 인프라 장애나 네트워크 끊김 발생 시 최대 2번 자동 재시도
    'retry_delay': timedelta(minutes=5), # 재시도 간격은 5분
}

# 2. DAG 정의 (우리의 파이프라인 이름 선언 및 스케줄링 타임 지정)
with DAG(
    'user_log_clipper_pipeline_dag',
    default_args=default_args,
    description='라이브클래스 강의 매출 및 유저 인기 분석 자동화 배치 파이프라인',
    schedule_interval='0 3 * * *', # [실무 핵심] 매일 새벽 3:00 정각에 배치가 자동으로 깨어나 실행됨 (Cron Expression)
    catchup=False, # 만약 에어플로우 서버가 꺼졌다 켜졌을 때 과거 밀린 배치를 무작위로 다 실행하는 것을 방지
    tags=['liveclass', 'analytics', 'bigquery'],
) as dag:

    # 3. Task 1: 증분 데이터 수집
    # 원천 데이터(DB)에서 어제 하루 치 로우 로그를 쏙 빼오는 단계
    task_extract = BashOperator(
        task_id='extract_yesterday_data',
        bash_command='python /Users/user/PycharmProjects/UserLogClipper/extract_db.py',
    )

    # 4. Task 2: 강의별 인기 및 MTD 누적 매출 가공
    # 수집된 데이터를 바탕으로 월별 누적 연산을 수행하는 엔진 가동
    task_transform = BashOperator(
        task_id='transform_course_analytics_mtd',
        bash_command='python /Users/user/PycharmProjects/UserLogClipper/task_course_analytics_mtd.py',
    )

    # 5. Task 3: 최종 가공 마트를 구글 빅쿼리로 전송 (Load)
    # 필드가 분리된 최종 테이블을 BigQuery 데이터 웨어하우스로 주입
    task_load = BashOperator(
        task_id='load_marts_to_bigquery',
        bash_command='python /Users/user/PycharmProjects/UserLogClipper/load_to_cloud.py',
    )

    # 6.  데이터 파이프라인 의존성(Workflow 순서) 지정
    # 수집이 끝나야 가공을 하고, 가공이 끝나야 빅쿼리에 적재합니다.
    task_extract >> task_transform >> task_load