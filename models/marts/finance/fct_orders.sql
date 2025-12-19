with orders as (
    select * from {{ref("stg_jaffle_shop__orders")}}
),
payments as (
    select * from {{ref("stg_stripe__payments")}}
),
order_payment as (
    select
        order_id,
        sum(case when status = 'success' then amount else 0 end) as amount
        from payments
        group by order_id
),
final as (
    select 
        p.order_id, 
        order_date,
        customer_id,
        coalesce(p.amount, 0) as amount
    from orders o
    left join order_payment p on p.order_id = o.order_id
)
select * from final