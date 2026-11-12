SELECT
    reference_month,
    vendor_id,
    COUNT(*) AS occurrences

FROM {{ ref('fct_monthly_vendor_performance') }}

GROUP BY
    reference_month,
    vendor_id

HAVING COUNT(*) > 1
