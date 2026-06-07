-- models/marts/mart_daily_course_sales_mtd.sql
-- 이번 달 누적 매출(MTD)
WITH staging_sales AS (
    SELECT * FROM {{ ref('stg_course_sales') }}
    WHERE payment_status = 'COMPLETED' -- 취소되지 않은 결제 완료 건만 필터링
),
    -- 일단위 강의별 매출 및 판매량 1차 필드 집계
daily_aggregation AS (

    SELECT
        sale_date,
        sale_year,
        sale_month,
        course_id,
        COUNT(order_id) AS daily_sales_count,
        SUM(price) AS daily_revenue
    FROM
        staging_sales
    GROUP BY
        1, 2, 3, 4
)
--  시계열 MTD 누적 매출 필드 생성
SELECT
    sale_date,
    sale_year,
    sale_month,
    course_id,
    daily_sales_count,
    daily_revenue,
    SUM(daily_revenue) OVER (
        PARTITION BY sale_year, sale_month, course_id 
        ORDER BY sale_date
    ) AS mtd_cumulative_revenue,
    SUM(daily_sales_count) OVER (
        PARTITION BY sale_year, sale_month, course_id 
        ORDER BY sale_date
    ) AS mtd_cumulative_sales_count
FROM
    daily_aggregation

