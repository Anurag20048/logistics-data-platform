select
    shipment_id,
    min(event_ts) as first_event_ts,
    max(event_ts) as last_event_ts,
    count(*) as event_count,
    count(distinct truck_id) as truck_count,
    max(distance_km) as distance_km,
    max(delay_minutes) as max_delay_minutes,
    sum(case when status = 'delayed' then 1 else 0 end) as delayed_events,
    case
      when count(*) > 0
      then round(sum(case when status = 'delayed' then 1 else 0 end)::numeric / count(*), 4)
      else 0
    end as delay_rate
from {{ ref('stg_logistics_events') }}
group by shipment_id
