-- Weather Statistics of NYC
SELECT `burrough` AS `burrough`,
       TIMESTAMP_TRUNC(`weather_timestamp`, YEAR) AS `weather_timestamp`,
       sum(`rain`) AS `Total_Rainfall`,
       sum(`snowfall`) AS `Total_Snowfall_52d24`,
       max(`snow_depth`) AS `Max_Snow_Depth_b0dc6`,
       AVG(`wind_gusts_10m`) AS `AVG_wind_gusts_619f2`,
       AVG(`wind_speed_10m`) AS `AVG_wind_speed_4345d`,
       AVG(`pressure_msl`) AS `AVG_pressure_msl_6e16d`,
       AVG(`dew_point_2m`) AS `AVG_dew_point_9a4bb`,
       AVG(`relative_humidity_2m`) AS `AVG_rel_humidity_30c0c`
FROM `df_prd`.`df_weather_data_prd`
GROUP BY `burrough`,
         `weather_timestamp`
ORDER BY `weather_timestamp` DESC


-- Fatality in High Impact Crashes
SELECT DATE_TRUNC(`crash_date`, MONTH) AS `crash_date`,
       `borough` AS `borough`,
       `was_fatal` AS `was_fatal`,
       count(`collision_id`) AS `COUNT_collision_id`
FROM `df_prd`.`df_unified_crashes_model`
WHERE `high_impact_crash` IN ('Yes')
  AND `crash_date` >= CAST('2016-01-01' AS DATE)
  AND `crash_date` < CAST('2024-05-12' AS DATE)
GROUP BY `crash_date`,
         `borough`,
         `was_fatal`
ORDER BY `COUNT_collision_id` DESC

-- Vehicles Involved in a Crash Year Over Year (YOY)
SELECT 
    EXTRACT(YEAR FROM crash_date) AS year,
    vehicle_type,
    COUNT(*) AS total
FROM 
    `df_prd.view_crashes_weather`
GROUP BY 
    year, vehicle_type;

-- Fatal Crashes in Weather Conditions
SELECT `snowfall_category` AS `snowfall_category`,
       `rainfall_category` AS `rainfall_category`,
       `borough` AS `borough`,
       count(`collision_id`) AS `Total_Collisions_01d63`
FROM `df_prd`.`df_crashes_weather_vw`
WHERE `was_fatal` IN (true)
GROUP BY `snowfall_category`,
         `rainfall_category`,
         `borough`

-- Body Injury and Status Post Crash
SELECT `bodily_injury` AS `bodily_injury`,
       `emotional_status` AS `emotional_status`,
       `age_category` AS `age_category`,
       count(DISTINCT `collision_id`) AS `COUNT_DISTINCT_collision_id__75f0b`
FROM `df_prd`.`df_persons_data_prd`
WHERE `emotional_status` IN ('Conscious',
                             'Unconscious',
                             'Semiconscious',
                             'Apparent Death',
                             'Shock',
                             'Incoherent')
  AND NOT (`bodily_injury` IS NULL
           OR `bodily_injury` IN ('Does Not Apply',
                                  'Unknown'))
GROUP BY `bodily_injury`,
         `emotional_status`,
         `age_category`

-- High Yielding Factors of Crashes
SELECT `person_contributing_factor_1` AS `person_contributing_factor_1`,
       count(`collision_id`) AS `COUNT_collision_id__039fa`
FROM `df_prd`.`df_crashes_weather_vw`
WHERE `person_contributing_factor_1` IS NOT NULL
  AND (`person_contributing_factor_1` NOT IN ('Pedestrian/Bicyclist/Other Pedestrian Error/Confusion',
                                              'Unknown',
                                              'Unspecified'))
GROUP BY `person_contributing_factor_1`
ORDER BY `COUNT_collision_id__039fa` DESC

-- Number of Persons Killed YoY
SELECT DATE_TRUNC(`crash_date`, YEAR) AS `crash_date`,
       `borough` AS `borough`,
       sum(`number_of_persons_killed`) AS `SUM_number_of_persons_killed__d85f2`
FROM `df_prd`.`df_unified_crashes_model`
GROUP BY `crash_date`,
         `borough`
ORDER BY `SUM_number_of_persons_killed__d85f2` DESC

-- Occurence of High Impact Crashes

SELECT DATE_TRUNC(`crash_date`, MONTH) AS `crash_date`,
       count(`collision_id`) AS `COUNT_collision_id__039fa`
FROM `df_prd`.`df_unified_crashes_model`
WHERE `high_impact_crash` IN ('Yes')
  AND `crash_date` >= CAST('2016-05-01' AS DATE)
  AND `crash_date` < CAST('2024-05-13' AS DATE)
GROUP BY `crash_date`

-- Number of Persons Injured
SELECT DATE_TRUNC(`crash_date`, MONTH) AS `crash_date`,
       sum(`number_of_persons_injured`) AS `SUM_number_of_persons_injured__9151f`
FROM `df_prd`.`df_unified_crashes_model`
WHERE `crash_date` >= CAST('2012-01-01' AS DATE)
  AND `crash_date` < CAST('2024-05-01' AS DATE)
GROUP BY `crash_date`

-- Contributing Factor for Crashes while turning
SELECT count(DISTINCT `collision_id`) AS `COUNT_DISTINCT_collision_id__75f0b`
FROM `df_prd`.`df_unified_crashes_model`
WHERE `high_impact_crash` IN ('Yes')
  AND `pre_crash` IN ('Making Left Turn',
                      'Making Right Turn on Red',
                      'Making Right Turn',
                      'Making Left Turn on Red',
                      'Making U Turn')

-- Categorizing Persons Involved in Crashes
SELECT `person_type` AS `person_type`,
       `age_category` AS `age_category`,
       count(`collision_id`) AS `Total_Cases_e585a`
FROM `df_prd`.`df_persons_data_prd`
WHERE (`age_category` NOT IN ('Unknown'))
GROUP BY `person_type`,
         `age_category`
ORDER BY `Total_Cases_e585a` DESC

-- Analyzing Traffic Speeds in NYC on weekdays and weekends
SELECT DATE_TRUNC(`crash_timestamp`, WEEK) AS `crash_timestamp`,
       `week_part` AS `week_part`,
       `speed_band` AS `speed_band`,
       AVG(`speed`) AS `AVG_speed__eea7c`
FROM `df_prd`.`df_traffic_data_prd`
GROUP BY `crash_timestamp`,
         `week_part`,
         `speed_band`
ORDER BY `AVG_speed__eea7c` DESC

-- Trend of High Impact Crashes YoY

SELECT DATE_TRUNC(`crash_date`, YEAR) AS `crash_date`,
       `borough` AS `borough`,
       count(`high_impact_crash`) AS `COUNT_high_impact_crash__cc698`
FROM `df_prd`.`df_unified_crashes_model`
GROUP BY `crash_date`,
         `borough`
ORDER BY `COUNT_high_impact_crash__cc698` DESC

-- Understanding pre crash condition and vehicle damage afterwards

SELECT `pre_crash` AS `pre_crash`,
       `vehicle_damage` AS `vehicle_damage`,
       count(`collision_id`) AS `Occurences`
FROM `df_prd`.`df_vehicles_data_prd`
WHERE `crash_date` >= CAST('2017-05-13' AS DATE)
  AND `crash_date` < CAST('2024-05-13' AS DATE)
  AND (`vehicle_damage` NOT IN ('Unknown',
                                'Other',
                                'No Damage'))
GROUP BY `pre_crash`,
         `vehicle_damage`

-- State-wise distribution of crashes

SELECT `state_registration` AS `state_registration`,
       count(`collision_id`) AS `COUNT_collision_id__039fa`
FROM `df_prd`.`df_vehicles_data_prd`
WHERE (`state_registration` NOT IN ('Unknown'))
  AND `crash_date` >= CAST('2023-01-01' AS DATE)
  AND `crash_date` < CAST('2024-05-13' AS DATE)
  AND `high_impact_crash` IN ('Yes')
GROUP BY `state_registration`