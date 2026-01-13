 -- with statement
 with 
    -- import ctes
    orders as ( 
        select * from {{ source('jaffle_shop', 'orders') }}
    ),
    customers as (
        select * from {{ source('jaffle_shop', 'customers') }}
    ),
    payments as (
        select * from {{ source('stripe', 'payment') }}
    ),
    -- logical ctes
    order_pyments as (
        select orderid as order_id, 
            max(created) as payment_finalized_date, 
            sum(amount) / 100.0 as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),
    paid_orders as (
        select orders.id as order_id,
            orders.user_id as customer_id,
            orders.order_date as order_placed_at,
            orders.status as order_status,
            order_pyments.total_amount_paid,
            order_pyments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join order_pyments on orders.id = order_pyments.order_id
        left join customers on orders.user_id = customers.id 
    ),
    lifetime_totals as (
        select
            order_id,
            sum (total_amount_paid) over (
                    partition by customer_id 
                    order by order_id 
                    range between unbounded preceding and current row
                ) as clv_bad
        from paid_orders
    ),
    -- final cte
    final as (
        select
            paid_orders.*,
            row_number() over (order by paid_orders.order_id) as transaction_seq,
            row_number() over (partition by customer_id order by paid_orders.order_id) as customer_sales_seq,
            case when ( rank () over (partition by paid_orders.customer_id 
                                        order by paid_orders.order_placed_at, paid_orders.order_id)) = 1 
                then 'new' 
                else 'return' 
            end as nvsr,
            lifetime_totals.clv_bad as customer_lifetime_value,
            min(paid_orders.order_placed_at) over (partition by paid_orders.customer_id) as fdos
            from paid_orders
            left outer join lifetime_totals on lifetime_totals.order_id = paid_orders.order_id
            order by order_id
    )
-- simple select statement
select * from final