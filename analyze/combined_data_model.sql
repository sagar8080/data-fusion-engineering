CREATE OR REPLACE TABLE `df_prd.df_unified_crashes_model` AS
WITH standardized_crashes AS (
    SELECT *,
        CASE
            WHEN LOWER(borough) IN ('manhattan', 'new york', 'new york city', 'ny') THEN 'Manhattan'
            WHEN LOWER(borough) IN ('brooklyn', 'kings', 'kings county') THEN 'Brooklyn'
            WHEN LOWER(borough) IN ('queens', 'queens county') THEN 'Queens'
            WHEN LOWER(borough) IN ('bronx', 'bronx county', 'bx') THEN 'Bronx'
            WHEN LOWER(borough) IN ('staten island', 'richmond', 'richmond county') THEN 'Staten Island'
            ELSE borough
        END AS standardized_borough
    FROM `df_prd.df_crashes_data_prd`
),
standardized_weather AS (
    SELECT *,
        DATE(date) as standardized_date,
        CASE
            WHEN LOWER(burrough) IN ('manhattan', 'new york', 'new york city', 'ny') THEN 'Manhattan'
            WHEN LOWER(burrough) IN ('brooklyn', 'kings', 'kings county') THEN 'Brooklyn'
            WHEN LOWER(burrough) IN ('queens', 'queens county') THEN 'Queens'
            WHEN LOWER(burrough) IN ('bronx', 'bronx county', 'bx') THEN 'Bronx'
            WHEN LOWER(burrough) IN ('staten island', 'richmond', 'richmond county') THEN 'Staten Island'
            ELSE burrough
        END AS standardized_burrough
    FROM `df_prd.df_weather_data_prd`
)
SELECT
    c.*,
    v.high_impact_crash,
    v.vehicle_year,
    v.vehicle_type,
    v.state_registration,
    v.pre_crash,
    v.vehicle_damage,
    v.contributing_factor AS vehicle_contributing_factor,
    p.person_type,
    p.person_injury,
    p.person_age,
    p.ejection,
    p.emotional_status,
    p.bodily_injury,
    p.position_in_vehicle,
    p.safety_equipment,
    p.contributing_factor_1 AS person_contributing_factor_1,
    p.contributing_factor_2 AS person_contributing_factor_2,
    p.person_sex
FROM standardized_crashes c
INNER JOIN `df_prd.df_vehicles_data_prd` v 
    ON c.collision_id = v.collision_id AND c.crash_date = v.crash_date
INNER JOIN `df_prd.df_persons_data_prd` p 
    ON c.collision_id = p.collision_id AND c.crash_date = p.crash_date

-- Create a View for Combined Crashes Model + Weather Data
CREATE OR REPLACE VIEW `df_prd.view_crashes_weather` AS
SELECT 
    cc.*,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.pressure_msl,
    w.wind_speed_10m,
    w.sunshine_duration
FROM 
    `df_prd.df_unified_crashes_model` cc
JOIN 
    `df_prd.df_weather_data_prd` w 
    ON cc.borough = w.burrough AND DATE(cc.crash_date) = w.date
WHERE 
    EXTRACT(YEAR FROM cc.crash_date) = @year;

-- Create a View for Combined Crashes Model + Traffic Data
CREATE OR REPLACE VIEW `df_prd.view_crashes_traffic` AS
SELECT 
    cc.*,
    t.speed,
    t.status,
    t.travel_time
FROM 
    `df_prd.df_unified_crashes_model` cc
JOIN 
    `df_prd.df_traffic_data_prd` t
    ON cc.borough = t.borough AND DATE(cc.crash_date) = t.crash_timestamp
WHERE 
    EXTRACT(YEAR FROM t.data_as_of) = @year;

-- Create a View for Traffic Data + Weather Data
CREATE OR REPLACE VIEW `df_prd.view_traffic_weather` AS
SELECT 
    t.*,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.pressure_msl,
    w.wind_speed_10m,
    w.sunshine_duration
FROM 
    `df_prd.df_traffic_data_prd` t
JOIN 
    `df_prd.df_weather_data_prd` w
    ON t.borough = w.burrough AND DATE(t.data_as_of) = w.date
WHERE 
    EXTRACT(YEAR FROM t.data_as_of) = @year;

