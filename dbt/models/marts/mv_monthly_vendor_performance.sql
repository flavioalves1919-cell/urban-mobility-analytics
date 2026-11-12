{{ config(materialized='materialized_view') }}

SELECT
    reference_month,
    vendor_id,
    total_trips,
    total_revenue,
    revenue_eligible_trips,
    average_fare,
    average_distance,
    average_trip_duration_minutes
FROM {{ ref('fct_monthly_vendor_performance') }}
