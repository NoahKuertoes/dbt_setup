WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
    ,date::DATE AS date_day
    ,EXTRACT(YEAR FROM date::DATE) AS year
    ,EXTRACT(MONTH FROM date::DATE) AS month
    ,EXTRACT(WEEK FROM date::DATE) AS cw
    ,TO_CHAR(date::DATE, 'Month') AS month_name
    ,TO_CHAR(date::DATE, 'Day') AS weekday
    FROM daily_data 
), 
add_more_features AS (
    SELECT *
		, (CASE 
			WHEN month in (12,1,2) THEN 'winter'
			WHEN month in (3,4,5) THEN 'spring'
            WHEN month in (6,7,8) THEN 'summer'
            WHEN month in (9,10,11) THEN 'autumn'
		END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date