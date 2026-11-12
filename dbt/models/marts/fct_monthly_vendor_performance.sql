SELECT
    reference_month,
    vendor_id,

    COUNT(*) AS total_trips,

    SUM(recognized_revenue) AS total_revenue,

    SUM(revenue_eligible_trip) AS revenue_eligible_trips,

    ROUND(AVG(fare_amount), 2) AS average_fare,

    ROUND(AVG(trip_distance), 2) AS average_distance,

    ROUND(AVG(trip_duration_minutes), 2) AS average_trip_duration_minutes

FROM {{ ref('int_yellow_taxi_trips') }}

GROUP BY
    reference_month,
    vendor_id
