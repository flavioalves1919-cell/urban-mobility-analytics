SELECT
    vendor_id,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
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
    END AS revenue_eligible_trip,

    EXTRACT(
        EPOCH FROM (
            tpep_dropoff_datetime - tpep_pickup_datetime
        )
    ) / 60 AS trip_duration_minutes

FROM {{ ref('stg_yellow_taxi_trips') }}
