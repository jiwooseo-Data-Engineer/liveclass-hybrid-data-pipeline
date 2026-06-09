import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import random

# 1. Docker PostgreSQL 연결 설정
try:
    conn = psycopg2.connect(
        host="localhost",
        port="5433",          
        database="superset",
        user="superset",
        password="superset"
    )
    cursor = conn.cursor()
    print(" PostgreSQL 연결 성공")
except Exception as e:
    print(f" DB 연결 실패: {e}")
    exit()

cursor.execute("DROP TABLE IF EXISTS daily_course_sales_marts;")
cursor.execute("DROP TABLE IF EXISTS user_connection_hourly_marts;")

cursor.execute("""
    CREATE TABLE daily_course_sales_marts (
        sale_date DATE,
        sale_year INT,
        sale_month INT,
        course_id VARCHAR(50),
        daily_sales_count INT,
        daily_revenue NUMERIC,
        mtd_cumulative_revenue NUMERIC,
        mtd_cumulative_sales_count INT
    );
""")

cursor.execute("""
    CREATE TABLE user_connection_hourly_marts (
        connection_id VARCHAR(50) PRIMARY KEY,
        user_id VARCHAR(50),
        visit_time TIMESTAMP,
        connection_hour INT,
        device VARCHAR(20),
        viewed_page VARCHAR(100)
    );
""")
conn.commit()

start_date = datetime(2026, 1, 1)
end_date = datetime.now()
delta_days = (end_date - start_date).days

courses = ['COURSE_JAVA_01', 'COURSE_SPRING_02', 'COURSE_DATA_ENG_03', 'COURSE_UIUX_04']
devices = ['MOBILE', 'DESKTOP']
pages = ['/course/detail/1', '/course/detail/2', '/course/detail/3', '/course/detail/4']

sales_data_list = []
course_cum_rev = {c: 0 for c in courses}
course_cum_cnt = {c: 0 for c in courses}

for i in range(delta_days + 1):
    current_date = start_date + timedelta(days=i)
    for course in courses:
        if course == 'COURSE_DATA_ENG_03':
            base_count = random.randint(15, 40)
            price = 150000
        elif course == 'COURSE_SPRING_02':
            base_count = random.randint(10, 30)
            price = 120000
        else:
            base_count = random.randint(5, 20)
            price = 90000
            
        sales_val = base_count if random.random() > 0.05 else 0  
        daily_rev = sales_val * price
        
        course_cum_rev[course] += daily_rev
        course_cum_cnt[course] += sales_val
        
        sales_data_list.append((
            current_date.date(), 
            current_date.year, 
            current_date.month, 
            course, 
            sales_val, 
            daily_rev, 
            course_cum_rev[course], 
            course_cum_cnt[course]
        ))

execute_values(cursor, """
    INSERT INTO daily_course_sales_marts 
    (sale_date, sale_year, sale_month, course_id, daily_sales_count, daily_revenue, mtd_cumulative_revenue, mtd_cumulative_sales_count)
    VALUES %s
""", sales_data_list)

# --- [이벤트 2 데이터 생성] 유저 시간대별 시청 패턴 ---
connection_data_list = []
used_conn_ids = set()

for i in range(10000): 
    rand_id = f"CONN_{random.randint(100000, 999999)}"
    if rand_id in used_conn_ids:
        continue
    used_conn_ids.add(rand_id)
    
    user_id = f"USER_{random.randint(1000, 5000)}"
    hour_choices = list(range(24))
    hour_weights = [2, 1, 1, 1, 1, 2, 4, 8, 15, 10, 7, 6, 8, 7, 6, 5, 7, 12, 18, 20, 15, 10, 6, 3]
    chosen_hour = random.choices(hour_choices, weights=hour_weights, k=1)[0]
    
    random_day_offset = random.randint(0, delta_days)
    visit_datetime = start_date + timedelta(days=random_day_offset, hours=chosen_hour, minutes=random.randint(0, 59))
    
    connection_data_list.append((rand_id, user_id, visit_datetime, chosen_hour, random.choice(devices), random.choice(pages)))

execute_values(cursor, """
    INSERT INTO user_connection_hourly_marts (connection_id, user_id, visit_time, connection_hour, device, viewed_page)
    VALUES %s
""", connection_data_list)

conn.commit()

cursor.close()
conn.close()