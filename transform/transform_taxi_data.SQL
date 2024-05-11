CREATE OR REPLACE TABLE `df_prd.df_taxi_data_prd_2019` AS
SELECT
  -- Standardizing vendor_id
  CASE vendor_id
    WHEN '1' THEN 'Creative Mobile Technologies, LLC'
    WHEN '2' THEN 'VeriFone Inc.'
    ELSE 'Other'
  END AS vendor_name,
  -- Keeping original pickup and dropoff timestamps
  pickup_datetime,
  dropoff_datetime,
  -- Calculating trip duration in minutes
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration_minutes,
  -- Passenger count
  passenger_count,
  -- Trip distance
  trip_distance,
  -- Decoding rate code
  CASE rate_code
    WHEN '1' THEN 'Standard rate'
    WHEN '2' THEN 'JFK'
    WHEN '3' THEN 'Newark'
    WHEN '4' THEN 'Nassau or Westchester'
    WHEN '5' THEN 'Negotiated fare'
    WHEN '6' THEN 'Group ride'
    ELSE 'Unknown rate code'
  END AS rate_description,
  -- Store and forward flag as boolean
  (store_and_fwd_flag = 'Y') AS is_store_and_forward,
  -- Standardizing payment type
  CASE payment_type
    WHEN '1' THEN 'Credit card'
    WHEN '2' THEN 'Cash'
    WHEN '3' THEN 'No charge'
    WHEN '4' THEN 'Dispute'
    WHEN '5' THEN 'Unknown'
    WHEN '6' THEN 'Voided trip'
    ELSE 'Other'
  END AS payment_method,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  imp_surcharge,
  airport_fee,
  total_amount,
  pickup_location_id,
  dropoff_location_id,
  data_file_year,
  data_file_month

FROM `df_raw.df_taxi_data_raw_2019`