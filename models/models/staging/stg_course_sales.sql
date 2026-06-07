-- models/staging/stg_course_sales.sql
WITH raw_sales AS (
    -- 원천 레이어에서 데이터를 읽어옴
    SELECT * FROM {{ source('liveclass_raw', 'mock_course_sales') }}
)

SELECT
    -- JSON/TEXT FIELT TYPE 분리 및 정형화
    CAST(order_id AS STRING) AS order_id,
    CAST(user_id AS STRING) AS user_id,
    CAST(course_id AS STRING) AS course_id,
    CAST(price AS NUMERIC) AS price,                  -- 금액은 NUMERIC 타입으로 필드 구분
    CAST(payment_status AS STRING) AS payment_status,
    CAST(sale_date AS DATE) AS sale_date,              -- 날짜는 DATE 타입으로 필드 구분
    EXTRACT(YEAR FROM CAST(sale_date AS DATE)) AS sale_year,
    EXTRACT(MONTH FROM CAST(sale_date AS DATE)) AS sale_month
FROM
    raw_sales