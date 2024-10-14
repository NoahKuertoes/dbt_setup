WITH hourly_data AS (
    SELECT * 
    FROM {{ref('staging_weather_hourly')}}
),
add_features AS (
    SELECT *
        , timestamp::DATE AS date -- date (without time)
        , timestamp::TIME AS time -- only time (hours:minutes:seconds) as TIME data type
        , TO_CHAR(timestamp,'HH24:MI') as hour -- time (hours:minutes) as TEXT data type
        , TO_CHAR(timestamp, 'FMmonth') AS month_name -- month name as a text
        , TO_CHAR(timestamp, 'FMDay') AS weekday -- weekday name as text
        , DATE_PART('day', timestamp) AS date_day -- day of the month
        , DATE_PART('month', timestamp) AS date_month -- month as number
        , DATE_PART('year', timestamp) AS date_year -- year as number
        , DATE_PART('week', timestamp) AS cw -- calendar week
    FROM hourly_data
),
add_more_features AS (
    SELECT *
        , (CASE 
            WHEN time BETWEEN '00:00:00' AND '06:00:00' THEN 'night'
            WHEN time BETWEEN '06:00:01' AND '12:00:00' THEN 'morning'
            WHEN time BETWEEN '12:00:01' AND '18:00:00' THEN 'afternoon'
            ELSE 'evening'
        END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features