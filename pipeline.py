import pandas as pd
import os


def run_user_log_clipper_pipeline():
    print("UserLogClipper 실무 배치를 시작.")

    # 1. 원천 데이터 불러오기
    if not os.path.exists('mock_user_connections.csv') or not os.path.exists('mock_course_sales.csv'):
        print("error:원천데이터 없음")
        return

    df_connections = pd.read_csv('mock_user_connections.csv')
    df_sales = pd.read_csv('mock_course_sales.csv')

    # 타임스탬프 컬럼들을 파이썬 datetime 객체로 변환 (시계열 분석 필수 과정)
    df_connections['visit_time'] = pd.to_datetime(df_connections['visit_time'])
    df_sales['created_at'] = pd.to_datetime(df_sales['created_at'])
    df_sales['sale_date'] = pd.to_datetime(df_sales['sale_date'])

    # [과제 1] 시간대별 유입 분석 마트 만들기
    # 방문 시간에서 '시(Hour)' 정보만 추출
    df_connections['visit_hour'] = df_connections['visit_time'].dt.hour

    # 시간대별로 방문 횟수(Traffic) 집계
    df_hourly_traffic = df_connections.groupby('visit_hour').size().reset_index(name='total_visits')
    # 유입이 가장 많은 순서대로 정렬
    df_hourly_traffic = df_hourly_traffic.sort_values(by='total_visits', ascending=False)

    # 결과 저장
    df_hourly_traffic.to_csv('mart_hourly_traffic.csv', index=False)
    print(" 과제 1 완료: 시간대별 유입 분석 마트 저장 (`mart_hourly_traffic.csv`)")

    # ==========================================
    # [과제 2] 일단위 강의 판매 및 월별 누적(MTD) 매출 마트 만들기
    # ==========================================
    # 실무 운영 관점: 환불/취소(CANCELLED)된 건은 매출 집계에서 제외해야 함 (정제 필터)
    df_completed_sales = df_sales[df_sales['payment_status'] == 'COMPLETED'].copy()

    # 연도와 월 컬럼 생성 (월별 누적 계산용)
    df_completed_sales['sale_year'] = df_completed_sales['sale_date'].dt.year
    df_completed_sales['sale_month'] = df_completed_sales['sale_date'].dt.month

    # 1단계: 일별 - 강의별 1차 집계 (Grain: Date - Course)
    df_daily_agg = df_completed_sales.groupby(['sale_date', 'sale_year', 'sale_month', 'course_id']).agg(
        daily_sales_count=('order_id', 'count'),
        daily_revenue=('price', 'sum')
    ).reset_index()

    # 시계열 누적을 위해 날짜 순으로 정렬
    df_daily_agg = df_daily_agg.sort_values(by=['course_id', 'sale_date']).reset_index(drop=True)

    # 2단계 [핵심 실무 로직]: 동일 연도, 동일 월, 동일 강의 내에서 일자별 누적 매출(MTD) 계산
    # SQL의 SUM(daily_revenue) OVER (PARTITION BY sale_year, sale_month, course_id ORDER BY sale_date)와 완벽히 동치
    df_daily_agg['mtd_cumulative_revenue'] = df_daily_agg.groupby(['sale_year', 'sale_month', 'course_id'])[
        'daily_revenue'].cumsum()
    df_daily_agg['mtd_cumulative_sales_count'] = df_daily_agg.groupby(['sale_year', 'sale_month', 'course_id'])[
        'daily_sales_count'].cumsum()

    # 결과 저장
    df_daily_agg.to_csv('mart_monthly_sales_trend.csv', index=False)
    print(" 과제 2 완료: 강의별 일차 집계 및 MTD 누적 마트 저장 (`mart_monthly_sales_trend.csv`)")
    print(" UserLogClipper 파이프라인 완료")


if __name__ == "__main__":
    run_user_log_clipper_pipeline()