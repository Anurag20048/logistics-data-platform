create schema if not exists analytics;

create table if not exists public.logistics_events (
    event_id varchar primary key,
    shipment_id varchar not null,
    truck_id varchar not null,
    customer_id varchar not null,
    event_type varchar not null,
    status varchar not null,
    delay_minutes integer not null,
    distance_km numeric(12,2) not null,
    event_ts timestamp not null
);
