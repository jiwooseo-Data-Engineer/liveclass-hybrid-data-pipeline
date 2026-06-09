import psycopg2
from datetime import datetime

# 1. 내 PC에서 Docker PostgreSQL(5433 포트)로 연결
conn = psycopg2.connect(
    host="localhost",
    port="5433",  # 💡 내 PC에서 접속할 때는 외부 포트인 5433을 씁니다!
    database="superset",
    user="superset",
    password="superset"
)
cursor = conn.cursor()

# 2. 프로젝트용 가상 유저 로그 테이블 생성 (예시)
cursor.execute("""
    CREATE TABLE IF NOT EXISTS user_activity_logs (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50),
        event_type VARCHAR(50),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
conn.commit()

# 3. 테스트용 샘플 데이터 삽입
sample_data = [
    ('user_01', 'login'),
    ('user_02', 'click_banner'),
    ('user_01', 'purchase'),
    ('user_03', 'login')
]

for user_id, event_type in sample_data:
    cursor.execute(
        "INSERT INTO user_activity_logs (user_id, event_type) VALUES (%s, %s);",
        (user_id, event_type)
    )

conn.commit()
print("🎉 테이블 생성 및 샘플 데이터 삽입 완료!")

cursor.close()
conn.close()