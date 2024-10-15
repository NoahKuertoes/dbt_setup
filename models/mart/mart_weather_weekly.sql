-- 5.4 Weekly weather
-- In a table mart_weather_weekly.sql we want to see all weather stats from the prep_weather_daily model aggregated weekly.
-- consider whether the metric should be Average, Maximum, Minimum, Sum or Mode
SELECT DISTINCT airport_code
    ,station_id
    ,round(avg(avg_temp_c),2) as avg_tmp_c
    ,round(min(min_temp_c),2) as min_temp_c
    ,round(max(max_temp_c),2) as max_temp_c
    ,round(sum(precipitation_mm),2) as precipitation_mm
    ,round(max(max_snow_mm),2) as max_snow_mm
    ,round(avg(avg_wind_direction),2) as avg_wind_direction
    ,round(avg(avg_wind_speed_kmh),2) as avg_wind_speed_kmh
    ,round(max(wind_peakgust_kmh),2) as wind_peakgust_kmh
    ,round(avg(avg_pressure_hpa),2) as avg_pressure_hpa
    ,round(avg(sun_minutes),2) as avg_sun_minutes
    ,year
    ,month
    ,cw
    ,month_name
    ,season
FROM {{ref('prep_weather_daily')}} 
GROUP BY cw, airport_code, station_id, year, month, month_name,season
ORDER BY cw