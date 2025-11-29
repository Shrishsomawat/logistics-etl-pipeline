-- This model cleans the raw data
SELECT
    shipment_id,
    origin_warehouse,
    destination_city,
    weight_kg,
    status,
    timestamp
FROM {{ source('postgres', 'raw_shipments') }}
WHERE weight_kg IS NOT NULL 
  AND weight_kg >= 0