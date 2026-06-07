-- ① 과제 1: 1월 1일부터 현재일까지 강의별 누적 매출 및 판매량 추이 조회
SELECT 
    sale_date,
    course_id,
    daily_revenue,
    mtd_cumulative_revenue -- 판다스/dbt 윈도우 함수로 연산된 누적 실적
FROM 
    `liveclass_marts.daily_course_sales_mtd`
WHERE 
    sale_date BETWEEN '2026-01-01' AND CURRENT_DATE()
ORDER BY 
    course_id, sale_date ASC;


-- ② 과제 2: 유저들이 "언제" 가장 많이 접속하고 콘텐츠를 소비했는지 피크 타임 분석
-- 통째로 저장되지 않고 쪼개진 visit_time의 'Hour(시간)' 필드를 기준으로 그룹핑 연산
SELECT 
    EXTRACT(HOUR FROM visit_time) AS connection_hour, -- 시간대 필드 분리
    COUNT(connection_id) AS total_views,               -- 해당 시간대 총 시청 건수
    COUNT(DISTINCT user_id) AS unique_users            -- 해당 시간대 순수 시청자 수
FROM 
    `liveclass_marts.user_connection_hourly`
GROUP BY 
    1
ORDER BY 
    total_views DESC; -- 가장 많이 본 시간대 순으로 정렬!