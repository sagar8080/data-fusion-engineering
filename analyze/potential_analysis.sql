--Weather Impact on Crashes by date
SELECT 
    cc.borough,
    cc.crash_date,
    COUNT(cc.collision_id) AS total_crashes,
    AVG(w.temperature_2m) AS average_temperature,
    AVG(w.wind_speed_10m) AS average_wind_speed
FROM 
    `df_prd.df_unified_crashes_model` cc
JOIN 
    `df_prd.df_weather_data_prd` w 
    ON cc.standardized_borough = w.burrough AND DATE(cc.crash_date) = DATE(w.date)
GROUP BY 
    cc.borough, cc.crash_date;


--Traffic conditions and number of crashes per day
SELECT 
    cc.crash_date,
    COUNT(cc.collision_id) AS total_crashes,
    AVG(t.speed) AS average_speed
FROM 
    `df_prd.df_unified_crashes_model` cc
JOIN 
    `df_prd.df_traffic_data_prd` t
    ON cc.standardized_borough = t.borough AND DATE(cc.crash_date) = DATE(t.crash_timestamp)
GROUP BY 
    cc.crash_date;

--Analysis of Contributing Factors in Crashes by borough
WITH ContributingFactors AS (
    SELECT 
        cc.borough,
        cc.vehicle_contributing_factor,
        COUNT(cc.collision_id) AS total_crashes,
        COUNT(cc.collision_id) * 1.0 / SUM(COUNT(cc.collision_id)) OVER (PARTITION BY cc.borough) AS percentage_of_total
    FROM 
        `df_prd.df_unified_crashes_model` cc
    GROUP BY 
        cc.borough, cc.vehicle_contributing_factor
),
RankedFactors AS (
    SELECT 
        borough,
        vehicle_contributing_factor,
        total_crashes,
        percentage_of_total,
        ROW_NUMBER() OVER (PARTITION BY borough ORDER BY total_crashes DESC) AS rank
    FROM 
        ContributingFactors
)
SELECT 
    borough,
    vehicle_contributing_factor,
    total_crashes,
    ROUND(percentage_of_total * 100, 2) AS percentage_of_total,
    rank
FROM 
    RankedFactors
WHERE 
    rank <= 5
ORDER BY 
    borough, rank;

-- Average vehicle age in crashes, by borough with key weather conditions
WITH VehicleAge AS (
    SELECT 
        cc.borough,
        v.vehicle_type,
        cc.number_of_persons_injured,
        cc.number_of_persons_killed,
        EXTRACT(YEAR FROM DATE(cc.crash_date)) - v.vehicle_year AS vehicle_age,
        w.temperature_2m,
        w.wind_speed_10m
    FROM 
        `df_prd.df_unified_crashes_model` cc
    JOIN 
        `df_prd.df_vehicles_data_prd` v 
        ON cc.collision_id = v.collision_id AND cc.crash_date = v.crash_date
    JOIN 
        `df_prd.df_weather_data_prd` w 
        ON cc.standardized_borough = w.burrough AND DATE(cc.crash_date) = DATE(w.date)
)
SELECT 
    borough,
    AVG(vehicle_age) AS average_vehicle_age,
    AVG(CASE WHEN number_of_persons_injured >= 3 THEN vehicle_age ELSE NULL END) AS average_vehicle_age_high_impact,
    vehicle_type,
    AVG(vehicle_age) AS average_vehicle_age_by_type,
    AVG(CASE WHEN number_of_persons_killed > 0 THEN vehicle_age ELSE NULL END) AS average_vehicle_age_with_fatalities,
    AVG(temperature_2m) AS average_temperature,
    AVG(wind_speed_10m) AS average_wind_speed
FROM 
    VehicleAge
GROUP BY 
    borough, vehicle_type
ORDER BY 
    borough, vehicle_type;
