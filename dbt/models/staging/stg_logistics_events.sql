select
    event_id,
    shipment_id,
    truck_id,
    customer_id,
    event_type,
    status,
    cast(delay_minutes as numeric) as delay_minutes,
    cast(distance_km as numeric) as distance_km,
    cast(event_ts as timestamp) as event_ts
from {{ source('raw', 'logistics_events') }}
