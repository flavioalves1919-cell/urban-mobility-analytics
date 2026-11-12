WITH gold_totals AS (
    SELECT
        SUM(total_trips) AS total_trips,
        SUM(revenue_eligible_trips) AS eligible_trips,
        SUM(total_revenue) AS total_revenue
    FROM {{ ref('fct_monthly_vendor_performance') }}
),

silver_totals AS (
    SELECT
        COUNT(*) AS total_trips,

        COUNT(*) FILTER (
            WHERE is_valid_revenue_payment
        ) AS eligible_trips,

        SUM(recognized_revenue) AS total_revenue

    FROM {{ ref('int_yellow_taxi_trips') }}
)

SELECT *
FROM gold_totals
CROSS JOIN silver_totals

WHERE gold_totals.total_trips
          IS DISTINCT FROM silver_totals.total_trips

   OR gold_totals.eligible_trips
          IS DISTINCT FROM silver_totals.eligible_trips

   OR gold_totals.total_revenue
          IS DISTINCT FROM silver_totals.total_revenue
