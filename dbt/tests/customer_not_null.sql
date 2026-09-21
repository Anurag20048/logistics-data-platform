select *
from {{ ref('customer_logistics_360') }}
where customer_id is null
