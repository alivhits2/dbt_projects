 -- with statement
 with 
    -- import ctes
    orders as ( 
        select * from {{ ref("stg_jaffle_shop__orders") }}
    ),
    customers as (
        select * from {{ ref("stg_jaffle_shop__customers") }}
    ),
    payments as (
        select * from {{ ref("stg_stripe__payments") }}
    ),
    -- logical ctes
    order_pyments as (
        select order_id, 
            max(created_at) as payment_finalized_date, 
            sum(amount) as total_amount_paid
        from payments
        where status <> 'fail'
        group by 1
    ),
    paid_orders as (
        select orders.order_id,
            orders.customer_id,
            orders.order_date as order_placed_at,
            orders.order_status,
            order_pyments.total_amount_paid,
            order_pyments.payment_finalized_date,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name
        from orders
        left join order_pyments on orders.order_id = order_pyments.order_id
        left join customers on orders.customer_id = customers.customer_id 
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
            sum (total_amount_paid) over (
                    partition by paid_orders.customer_id 
                    order by paid_orders.order_id 
                    range between unbounded preceding and current row
                ) as customer_lifetime_value_2,
            min(paid_orders.order_placed_at) over (partition by paid_orders.customer_id) as fdos
            from paid_orders
            order by order_id
    )
-- simple select statement
select * from final