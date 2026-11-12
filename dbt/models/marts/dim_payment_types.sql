SELECT
    payment_type_id,
    payment_type_name
FROM {{ ref('payment_types') }}
