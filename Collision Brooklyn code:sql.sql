
--------------------------------================START================---------------------------------



--------====SLIDE 1 (Executive Summary)

-- borough by collision rate
SELECT borough, count(unique_key)  cnt 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough is not null
group by borough  
order by 2 desc 

--Top 3 factors
SELECT contributing_factor_vehicle_1, count(unique_key) cnt 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
WHERE borough = 'BROOKLYN'
group by contributing_factor_vehicle_1  
order by 2 desc 

---deaths & injuries rate
select year, 
sum(number_of_cyclist_injured)  cyclist_injured, 
sum(number_of_motorist_injured) motorist_injured, 
sum(number_of_pedestrians_injured) pedestrians_injured,
sum(number_of_persons_injured) persons_injured,
sum(number_of_cyclist_killed) cyclist_killed, 
sum(number_of_motorist_killed) motorist_killed,
sum(number_of_pedestrians_killed) pedestrians_killed
sum(number_of_persons_killed) persons_killed,
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 


--Top3 vehicle type
select vehicle_type_code1 veh_type, count(unique_key) cnt 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
and vehicle_type_code1 is not null
group by vehicle_type_code1
order by 2 desc
limit 10


----Averages crashes per day
select year,  AVG(daily_cnt) avg_cnt from(
select EXTRACT(YEAR from timestamp) as year,FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME)) AS date, count(unique_key) daily_cnt 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
group by EXTRACT(YEAR from timestamp), FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME))
) group by year
order by 2






--------====SLIDE 2 (Analysis approach)

----- Duplicate checks, Timestamp atribute check, Null records & data exploration
SELECT  count(unique_key) cnt , count(distinct unique_key) dist_cnt
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
--WHERE borough = 'BROOKLYN' -- 391,772

SELECT  count(*) , unique_key
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
--WHERE borough = 'BROOKLYN' -- 391,772
group by unique_key
having count(*) > 1

SELECT  count(unique_key) cnt, 
case when contributing_factor_vehicle_1 is not null then 'AVAILABLE' else 'N/A' end contributing_factor_vehicle,
case when EXTRACT(DATE from timestamp) is not null then 'AVAILABLE' else 'N/A' end timestamp_check
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
WHERE borough = 'BROOKLYN'
group by case when contributing_factor_vehicle_1 is not null then 'AVAILABLE' else 'N/A' end,
case when EXTRACT(DATE from timestamp) is not null then 'AVAILABLE' else 'N/A' end



-------- Data cleaning & enrichment
SELECT  timestamp, 
FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME)) AS date,
FORMAT_DATETIME('%X', CAST(timestamp AS DATETIME)) AS time, 
EXTRACT(YEAR from timestamp) as year, EXTRACT(MONTH from timestamp) as mon, EXTRACT(DAY from timestamp) as day,
EXTRACT(HOUR from timestamp) as hr, 
FORMAT_DATETIME('%A', CAST(timestamp AS DATETIME)) AS weekday, 
FORMAT_DATETIME('%W', CAST(timestamp AS DATETIME)) AS yweek_num, 
FORMAT_DATETIME('%B', CAST(timestamp AS DATETIME)) AS month,
borough, zip_code,latitude, longitude, location,
vehicle_type_code1 vehicle_type, contributing_factor_vehicle_1 collision_factor, unique_key,
number_of_cyclist_injured, number_of_cyclist_killed, number_of_motorist_injured, number_of_motorist_killed,
number_of_pedestrians_injured, number_of_pedestrians_killed, number_of_persons_injured, number_of_persons_killed 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 








--------====SLIDE 3 (Key Insights)

---- Insight by cases or factors that lead to collision over the years (Top 5)

-- yearly
select year, collision_factor, cnt, rank from (
select year, collision_factor, cnt,
rank() over(partition by year order by cnt desc) as rank
from (
select year, collision_factor, count(unique_key) cnt
from (
SELECT  timestamp, 
FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME)) AS date,
FORMAT_DATETIME('%X', CAST(timestamp AS DATETIME)) AS time, 
EXTRACT(YEAR from timestamp) as year, EXTRACT(MONTH from timestamp) as mon, EXTRACT(DAY from timestamp) as day,
EXTRACT(HOUR from timestamp) as hr, 
FORMAT_DATETIME('%A', CAST(timestamp AS DATETIME)) AS weekday, 
FORMAT_DATETIME('%W', CAST(timestamp AS DATETIME)) AS yweek_num, 
FORMAT_DATETIME('%B', CAST(timestamp AS DATETIME)) AS month,
borough, zip_code,latitude, longitude, location,
vehicle_type_code1 vehicle_type, contributing_factor_vehicle_1 collision_factor, unique_key,
number_of_cyclist_injured, number_of_cyclist_killed, number_of_motorist_injured, number_of_motorist_killed,
number_of_pedestrians_injured, number_of_pedestrians_killed, number_of_persons_injured, number_of_persons_killed 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
)
group by year, collision_factor
)
) where rank <=5
order by year desc, rank asc


OR 

--Overall
select borough, collision_factor, count(unique_key) cnt
from (
SELECT  timestamp, 
FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME)) AS date,
FORMAT_DATETIME('%X', CAST(timestamp AS DATETIME)) AS time, 
EXTRACT(YEAR from timestamp) as year, EXTRACT(MONTH from timestamp) as mon, EXTRACT(DAY from timestamp) as day,
EXTRACT(HOUR from timestamp) as hr, 
FORMAT_DATETIME('%A', CAST(timestamp AS DATETIME)) AS weekday, 
FORMAT_DATETIME('%W', CAST(timestamp AS DATETIME)) AS yweek_num, 
FORMAT_DATETIME('%B', CAST(timestamp AS DATETIME)) AS month,
borough, zip_code,latitude, longitude, location,
vehicle_type_code1 vehicle_type, contributing_factor_vehicle_1 collision_factor, unique_key,
number_of_cyclist_injured, number_of_cyclist_killed, number_of_motorist_injured, number_of_motorist_killed,
number_of_pedestrians_injured, number_of_pedestrians_killed, number_of_persons_injured, number_of_persons_killed 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
)
group by borough, collision_factor
order by 3 desc
limit 10



---- Insight by cases or factors that lead to collision (Drilldown to time of the day / day of the week/ month within the year)

select year, month, weekday, hr, count(unique_key) cnt
from (
SELECT  timestamp, 
FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME)) AS date,
FORMAT_DATETIME('%X', CAST(timestamp AS DATETIME)) AS time, 
EXTRACT(YEAR from timestamp) as year, EXTRACT(MONTH from timestamp) as mon, EXTRACT(DAY from timestamp) as day,
EXTRACT(HOUR from timestamp) as hr, 
FORMAT_DATETIME('%A', CAST(timestamp AS DATETIME)) AS weekday, 
FORMAT_DATETIME('%W', CAST(timestamp AS DATETIME)) AS yweek_num, 
FORMAT_DATETIME('%B', CAST(timestamp AS DATETIME)) AS month,
borough, zip_code,latitude, longitude, location,
vehicle_type_code1 vehicle_type, contributing_factor_vehicle_1 collision_factor, unique_key,
number_of_cyclist_injured, number_of_cyclist_killed, number_of_motorist_injured, number_of_motorist_killed,
number_of_pedestrians_injured, number_of_pedestrians_killed, number_of_persons_injured, number_of_persons_killed 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
)
--where year = 2019
group by year, month, weekday, hr



---- Insight by Casualities caused by collision over the years

select year, 
sum(number_of_cyclist_injured)  cyclist_injured, 
sum(number_of_motorist_injured) motorist_injured, 
sum(number_of_pedestrians_injured) pedestrians_injured,
sum(number_of_cyclist_killed) cyclist_killed, 
sum(number_of_motorist_killed) motorist_killed,
sum(number_of_pedestrians_killed) pedestrians_killed
from (
SELECT  timestamp, 
FORMAT_DATETIME('%F', CAST(timestamp AS DATETIME)) AS date,
FORMAT_DATETIME('%X', CAST(timestamp AS DATETIME)) AS time, 
EXTRACT(YEAR from timestamp) as year, EXTRACT(MONTH from timestamp) as mon, EXTRACT(DAY from timestamp) as day,
EXTRACT(HOUR from timestamp) as hr, 
FORMAT_DATETIME('%A', CAST(timestamp AS DATETIME)) AS weekday, 
FORMAT_DATETIME('%W', CAST(timestamp AS DATETIME)) AS yweek_num, 
FORMAT_DATETIME('%B', CAST(timestamp AS DATETIME)) AS month,
borough, zip_code,latitude, longitude, location,
vehicle_type_code1 vehicle_type, contributing_factor_vehicle_1 collision_factor, unique_key,
number_of_cyclist_injured, number_of_cyclist_killed, number_of_motorist_injured, number_of_motorist_killed,
number_of_pedestrians_injured, number_of_pedestrians_killed, number_of_persons_injured, number_of_persons_killed 
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
)
group by year


--Areas in Brooklyn with more cases
select zip_code, 
sum(number_of_persons_injured)  injured, 
sum(number_of_persons_killed)  killed,
count(unique_key) collision_cnt
FROM `bigquery-public-data.new_york_mv_collisions.nypd_mv_collisions` 
where borough = 'BROOKLYN' 
and zip_code is not null
group by zip_code
order by 3 desc



--------------------------------===============END=================---------------------------------







