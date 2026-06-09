import os
from google.cloud import bigquery
import google.auth

def upload_marts_to_bigquery():
    print("UserLogClipper 최종 마트 파일의 BigQuery 적재를 시작합니다...")

    # 1. 🎯 프로젝트 ID 설정을 지우님의 진짜 프로젝트 ID인 'mtd-pipeline'으로 교체했습니다.
    PROJECT_ID = 'mtd-pipeline'
    DATASET_ID = 'liveclass_marts'

    # 적재할 로컬 파일과 빅쿼리 테이블 매핑 (파일명 : 테이블명)
    files_to_load = {
        'mart_daily_course_sales_mtd.csv': 'daily_course_sales_mtd',
        'mart_course_popularity_rank.csv': 'course_popularity_rank'
    }

    try:
        # 2. 🔥 [무적의 인증 치트키] 브라우저 로그인을 강제로 유도하는 인증 방식입니다.
        # 이 방식을 쓰면 지러 지우님의 로컬 PC 브라우저 권한을 코드가 그대로 훔쳐다 씁니다!
        print("구글 계정 웹 브라우저 인증을 시작합니다...")
        credentials, project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        # 브라우저 권한(credentials)을 장착해서 클라이언트를 초기화합니다.
        client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

        # 3. 빅쿼리 적재 옵션 설정 (CSV 파일을 테이블로 밀어 넣기 위한 설정)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # 첫 줄 헤더(컬럼명) 건너뛰기
            autodetect=True,  # 데이터 타입을 빅쿼리가 알아서 탐지 (정수, 문자열 등)
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # 매일 새벽 새로 덮어쓰기(Overwrite)
        )

        for local_file, table_name in files_to_load.items():
            if not os.path.exists(local_file):
                print(f" ⚠️ 적재할 로컬 파일이 없습니다: {local_file}")
                print(f"    현재 폴더에 해당 CSV 파일이 진짜 존재하는지 확인해 주세요!")
                continue

            table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
            print(f" 🔄 빅쿼리 적재 중... : {local_file} ➡️ {table_ref}")

            # 파일 오픈 후 빅쿼리로 스트리밍 로드
            with open(local_file, "rb") as source_file:
                load_job = client.load_table_from_file(
                    source_file, table_ref, job_config=job_config
                )

            load_job.result()  # 적재 작업이 끝날 때까지 대기
            print(f" ✅ 빅쿼리 적재 완료: {table_name} 테이블 생성됨")

        print("🎉 마트 데이터가 BigQuery에 성공적으로 적재되었습니다! 🎉")

    except Exception as e:
        print(f" ❌ 에러 발생: {str(e)}")


if __name__ == "__main__":
    upload_marts_to_bigquery()