import pandas as pd
import os

# 1. 파일 존재 여부 확인
if not os.path.exists('mock_course_sales.csv') or not os.path.exists('mock_user_connections.csv'):
    print("원천 데이터 파일이 부족합니다.")
    exit()

# 2. 데이터 로드 및 날짜 정형화
df_sales = pd.read_csv('mock_course_sales.csv')
df_connections = pd.read_csv('mock_user_connections.csv')

df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'])
df_sales['sale_year'] = df_sales['sale_date'].dt.year
df_sales['sale_month'] = df_sales['sale_date'].dt.month

# 3. 매출 및 판매량 집계: 취소되지 않은 결제 완료 데이터만 필터링
df_completed = df_sales[df_sales['payment_status'] == 'COMPLETED'].copy()

df_daily_sales = df_completed.groupby(['sale_date', 'sale_year', 'sale_month', 'course_id']).agg(
    daily_sales_count=('order_id', 'count'),
    daily_revenue=('price', 'sum')
).reset_index()


# 4. 시계열 MTD 누적 계산 각 월별로 강의 매출이 누적되도록 설정
df_daily_sales = df_daily_sales.sort_values(by=['course_id', 'sale_date']).reset_index(drop=True)
df_daily_sales['mtd_cumulative_revenue'] = df_daily_sales.groupby(['sale_year', 'sale_month', 'course_id'])['daily_revenue'].cumsum()

# 5. 유저 반응/인기 지표 결합
# 원천 접속 데이터가 '어떤 강의'를 본 것인지 매칭되어 있다고 가정하고 집계하거나, 결제 데이터의 흐름과 매칭하기 위해 일별 강의별 총 판매 효율을 연결합니다.
print("강의별 매출 시계열 마트 가공")

# 6. 최종 인사이트 도출 과거 전체 기간 및 현재 달 기준 '가장 반응이 좋은 강의' 순위 마트 생성
# 이 테이블을 보면 어떤 강의가 매출이 높고 낮은지, 판매 속도가 어떤지 한눈에 알 수 있습니다.
df_course_summary = df_daily_sales.groupby('course_id').agg(
    total_revenue=('daily_revenue', 'sum'),
    total_sales_count=('daily_sales_count', 'sum'),
    avg_daily_revenue=('daily_revenue', 'mean')
).reset_index()

# 매출이 높은 순서대로 정렬 (이강의가 가장 인기가 있고 반응이 좋다는 증거)
df_course_summary = df_course_summary.sort_values(by='total_revenue', ascending=False)

# 7. 마트 파일 저장
df_daily_sales.to_csv('mart_daily_course_sales_mtd.csv', index=False)
df_course_summary.to_csv('mart_course_popularity_rank.csv', index=False)

print("일단위 시계열 누적 마트 저장 (`mart_daily_course_sales_mtd.csv`)")
print("강의별 최종 인기 순위 리포트 저장 (`mart_course_popularity_rank.csv`)")