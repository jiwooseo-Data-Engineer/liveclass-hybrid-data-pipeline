👨‍🏫 프로젝트 소개

파이프라인을 만들때 지원한 라이프클래스에서 발생가능한 인프레이션이나 클릭등 유저의 이벤트로그 관련되서 회사에 필요하고 발생할만한 이벤트가 어떤게 있을까를 고민했습니다.
그래서 2가지의 가설을 가지고 필요한 가상데이터를 만들고,
실시간,배치성 파이프라인을 만들기로 결정하였습니다.

첫 번째 이벤트 유형 : 1월 1일부터 현재일까지 강의별 매출 (MTD ~ YTD),

두번째 이벤트 유형: 하루중 사용자들이 "언제" 강의를 가장 많이 듣고 "언제"가 가장 적게 듣는가?

## 전체 아키텍쳐의 흐름

⏲️ 개발 기간
2026.06.07(토) ~ 2023.06.09(화)

🎯 기술 스택
- Core Languages: Python, SQL
- Data Warehouse : GCP Bigquery
- Orchestration & Automation : Apache Airflow
- BI & Visualization : Apache Superset
- realstreming : Apache kafka
- Container : Docker / Docker Compose

📝 프로젝트 아키텍쳐


[Raw Layer] ──> 1) 실시간 로그  ──> [Kafka Stream] ──> [Consumer/dbt stg] ──> [BigQuery Marts] ──> [Apache Superset]
            ──> 2) 일일 결제배치 ──> [Extract/dbt stg] ──> [Pandas/cumsum]   ──┘ (Storage & Serving)
                 (Visualization)


실시간 스트리밍 라인 (Streaming Pipeline):
유저가 행동 로그를 남기는 즉시 Kafka Producer를 통해 이벤트가 발행되며, Kafka Consumer가 이를 24시간 실시간으로 감지하고 정제합니다. 비정형 JSON 로그는 dbt 스타일의 스테이징 구조를 거쳐 즉시 연산 가능한 정형 데이터로 분리된 후 빅쿼리에 스트리밍 적재됩니다.

일일 증분 배치 라인 (Batch Pipeline):
매일 새벽 3시, Airflow 환경을 모킹한 스케줄러에 의해 load_to_cloud.py.py가 가동됩니다. 전체 데이터를 매번 풀 스캔(Full Scan)하는 비용 낭비를 지양하고, 어제 하루 치의 변경 사항만 가져오는 증분 수집 방식을 채택했습니다. 수집된 데이터는 Pandas의 cumsum() 연산을 통해 일일 매출 및 월 누적 매출(MTD) 마트로 변환되어 빅쿼리에 적재됩니다.

![alt text](./images/Project_Architecture.png)

전체 아키텍쳐의 흐름

0. Infrastructure & Containerization (Docker Compose)

- docker-compose.yml

설명: Zookeeper, Kafka, Superset 컨테이너를 코드 기반(IaC)으로 선언하여 명령어 한 줄(docker compose up)로 전체 인프라 환경을 복구 및 확장할 수 있도록 설계했습니다.

1. Raw Layer (원천 데이터 생성)

- mock_course_sales.csv, mock_user_connections.csv

설명: 웹사이트에서 유저들이 밤새 남긴 가공되지 않은 행동 로그와 결제 트랜잭션 데이터가 생성되는 지점입니다. (image_0.png, image_1.png의 데이터들입니다.)

2. Ingestion Layer (Streaming - Kafka 생성):

- kafka_producer.py, models/streaming/ (stg_kafka_sales.sql)

설명: 유저의 결제 이벤트가 발생하는 즉시 kafka_producer.py를 통해 Kafka 토픽으로 실시간 발행됩니다. 이와 동시에 dbt 스타일의 스테이징 모델(models/streaming/)을 통해 실시간 JSON 로그를 필드별로 명확히 정형화하는 설계를 확립했습니다.

쉽게 설명하면 문자열 데이터를 카프카로 유실 없이 빠르게 받아내고, dbt의 규칙을 적용해 컴퓨터가 즉시 연산할 수 있는 숫자와 날짜 Column으로 나누어 저장했습니다.

3. Ingestion & Staging Layer (Batch - 수집):

- extract_db.py, models/batch/ (stg_course_sales.sql, stg_user_connections.sql)

설명: 스케줄러에 의해 매일 새벽 3시에 배치가 실행되면, extract_db.py가 전체 데이터 중 어제 하루 치 데이터만 증분 수집(Incremental Extract)해 옵니다. 마찬가지로 dbt 스타일의 스테이징 모델을 통해 배치 CSV 데이터를 정형화하는 설계를 확립했습니다.

4. Transformation Layer:

- 01_task_course_analytics_MTD.py, kafka_consumer.py, models/marts/ (mart_daily_course_sales_mtd.sql, marts.yml)

설명: Streaming (Kafka Consumer): kafka_consumer.py가 Kafka 토픽을 24시간 감시하다 데이터가 들어오면 실시간으로 수집 및 정제합니다. 

Batch (Pandas & BigQuery): 수집된 어제 데이터를 바탕으로 취소 건 필터링, 판다스(cumsum) 및 빅쿼리를 활용해 강의별 일일 매출과 월 누적 매출(MTD)을 계산하여 분석 마트 데이터로 변환합니다.

5. Serving & Analytics Layer:

- load_to_cloud.py, analysis_queries.sql

설명: 가공이 끝난 깨끗한 마트 데이터를 Google BigQuery 데이터 웨어하우스에 적재합니다.(Streaming API 및 Batch Load 방식 혼용)

필드가 완전히 분리되어 있으므로, 현업 담당자들은 analysis_queries.sql을 이용해 인기가 많고 적은 강의를 쿼리해 갈 수 있습니다.

6. Analytics (Business Value - Superset 시각화):

설명: 빅쿼리에 적재된 정형화 마트 데이터를 Apache Superset 대시보드에 직접 연결하여, 과거 전체 흥행 순위뿐만 아니라 당일 매출의 MTD 누적 추이까지 차트로 시각화하여 비즈니스 가치를 창출합니다. (대시보드 구축은 GUI에서 진행되므로 코드가 없습니다.)

📊 Superset BI 

## 📊 1. 강의별 전체 매출 분석 (일배치 및 전체매출 추이)
강의별 전체 매출분석을 일자별,2026년 1월1일~2026년 6월9일까지 매출기준으로 집계하였습니다.(일일,전체 매출)
![alt text](./images/Total_sales_by_course.png)
![alt text](./images/Daily_total_sales by_lecture.png)


## 📊 2. 유저 시간대별 접속 로그 분석 (인프라 및 마케팅 인사이트)
플랫폼 내 유저들의 실시간 행동 로그를 1시간 단위로 집계하여 시간대별 트래픽 패턴을 분석하고 시각화했습니다.
![alt text](./images/user_lacture_log.png)
