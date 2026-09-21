select
    customer_id,
    count(*) as event_count,
    count(distinct shipment_id) as shipment_count,
    count(distinct truck_id) as truck_count,
    sum(distance_km) as total_distance_km,
    avg(delay_minutes) as avg_delay_minutes,
    max(delay_minutes) as max_delay_minutes,
    sum(case when status = 'delayed' then 1 else 0 end) as delayed_events,
    sum(case when event_type = 'delivered' then 1 else 0 end) as delivered_events,
    round(
        sum(case when status = 'delayed' then 1 else 0 end)::numeric
        / nullif(count(*), 0), 4
    ) as delay_rate,
    round(
        sum(case when event_type = 'delivered' then 1 else 0 end)::numeric
        / nullif(count(*), 0), 4
    ) as delivery_rate,
    max(event_ts) as last_event_ts
from {{ ref('stg_logistics_events') }}
group by customer_id
