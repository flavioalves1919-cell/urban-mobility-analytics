SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    pickup_date,
    pickup_year_month,
    trip_duration_minutes,
    is_valid_trip,
    invalid_reason,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    total_amount,
    reference_month,
    quality_status,

    is_valid_revenue_payment,

    CASE
        WHEN is_valid_revenue_payment
            THEN total_amount
        ELSE 0
    END AS recognized_revenue,

    CASE
        WHEN is_valid_revenue_payment
            THEN 1
        ELSE 0
    END AS revenue_eligible_trip

FROM {{ ref('stg_yellow_taxi_trips') }}
