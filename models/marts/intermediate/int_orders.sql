 with 
    orders as ( 
        select * from {{ ref("stg_jaffle_shop__orders") }}
    ),
    payments as (
        select * from {{ ref("stg_stripe__payments") }}
    ),
    order_totals as (
        select order_id, 
            max(created_at) as payment_finalized_date, 
            sum(amount) as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),
    transformed as (
        select orders.order_id,
            orders.customer_id,
            orders.order_date as order_placed_at,
            orders.order_status,
            order_totals.total_amount_paid,
            order_totals.payment_finalized_date
        from orders
        left join order_totals on orders.order_id = order_totals.order_id
    )
select * from transformed