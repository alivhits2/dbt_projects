select order_id, sum(case when status = 'success' then amount else 0 end) as total
from {{ref("stg_stripe__payments")}}
group by order_id
having total < 0