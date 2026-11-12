SELECT *
FROM {{ ref('fct_monthly_vendor_performance') }}

WHERE revenue_eligible_trips > total_trips
