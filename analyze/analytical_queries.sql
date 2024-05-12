-- Weather Statistics of NYC
SELECT 
    burrough,
    EXTRACT(YEAR FROM date) AS year,
    AVG(sunshine_duration) AS average_sunshine_in_minutes,
    AVG(mean_temp) AS mean_temp_in_degrees_c,
    AVG(humidity_level) AS mean_humidity_level,
    AVG(pressure_msl) AS average_pressure_msl,
    AVG(wind_speed_10m) AS average_wind_speed
FROM 
    `df_prd.view_crashes_weather`
GROUP BY 
    burrough, year;


-- Traffic Speed in NYC on Weekends and Weekdays
SELECT 
    EXTRACT(YEAR FROM data_as_of) AS year,
    EXTRACT(WEEK FROM data_as_of) AS week,
    AVG(speed) AS average_speed,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM data_as_of) IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM 
    `df_prd.view_crashes_weather`
GROUP BY 
    year, week, day_type;

-- Vehicles Involved in a Crash Year Over Year (YOY)
SELECT 
    EXTRACT(YEAR FROM crash_date) AS year,
    vehicle_type,
    COUNT(*) AS total
FROM 
    `df_prd.view_crashes_weather`
GROUP BY 
    year, vehicle_type;

-- High Impact Crashes Year Over Year
SELECT 
    EXTRACT(YEAR FROM crash_date) AS year,
    borough,
    COUNT(*) AS high_impact_crashes
FROM 
    `df_prd.df_unified_crashes_model`
WHERE 
    number_of_persons_injured >= 3
GROUP BY 
    year, borough;

-- Number of Persons Injured
SELECT 
    EXTRACT(DAY FROM crash_date) AS day,
    COUNT(number_of_persons_injured) AS total_injured
FROM 
    `df_prd.df_unified_crashes_model`
WHERE 
    EXTRACT(YEAR FROM crash_date) = 2024 AND EXTRACT(MONTH FROM crash_date) = 4
GROUP BY 
    day;

-- Number of Pedestrians Injured and Contributing Factors
SELECT 
    contributing_factor_vehicle_1 AS contributing_factor,
    COUNT(*) AS number_of_pedestrians_injured
FROM 
    `df_prd.df_unified_crashes_model`
WHERE 
    number_of_pedestrians_injured > 0
GROUP BY 
    contributing_factor;

-- Number of Persons Killed Year Over Year
SELECT 
    EXTRACT(YEAR FROM crash_date) AS year,
    SUM(number_of_persons_killed) AS total_killed
FROM 
    `df_prd.df_unified_crashes_model`
GROUP BY 
    year;

-- Fatality in High Impact Crashes
SELECT 
    EXTRACT(YEAR FROM crash_date) AS year,
    borough,
    SUM(number_of_persons_killed) AS fatalities
FROM 
    `df_prd.df_unified_crashes_model`
WHERE 
    number_of_persons_injured >= 3
GROUP BY 
    year, borough;