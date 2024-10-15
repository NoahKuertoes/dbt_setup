WITH routes AS (
-- we want to see for each route over all time:
    SELECT CONCAT(origin, '-', dest) AS route
        --origin airport code
        ,origin 
        -- destination airport code
        ,dest
        --total flights on this route
        ,count(DISTINCT flight_number) as n_flights
        --unique airplanes
        ,count(DISTINCT tail_number) as n_planes
        --unique airlines
        ,count(DISTINCT airline) as n_airlines
        --on average what is the actual elapsed time
        ,round(avg(actual_elapsed_time),2) as average_elapsed_time
        --on average what is the delay on arrival
        ,round(avg(arr_delay),2) as average_delay_arr
        --what was the max delay?
        ,max(arr_delay) as max_arr_delay
        --what was the min delay?
        ,min(arr_delay) as min_delay
        --total number of cancelled
        ,sum(cancelled) as total_cancelled
        --total number of diverted
        ,sum(diverted) as total_diverted
    FROM {{ref('prep_flights')}}
    GROUP BY origin, dest
-- add city, country and name for both, origin and destination, airports 
),add_dest_info as (
    SELECT pa.name as dest_name
        ,pa.city as dest_city
        ,pa.country as dest_country
        ,r.*
    FROM routes r
    join {{ref('prep_airports')}} pa on pa.faa = r.dest
),routes_all as(
    SELECT pa.name as origin_name
    ,pa.city as origin_city
    ,pa.country as origin_country
    ,d.*
    FROM add_dest_info d
    JOIN {{ref('prep_airports')}} pa on pa.faa = d.origin
)
SELECT * FROM routes_all

