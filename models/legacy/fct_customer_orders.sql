 -- with statement
 with 
    -- import ctes
    customers as (
        select * from {{ ref("stg_jaffle_shop__customers") }}
    ),
    orders as (
        select * from {{ ref("int_orders") }}
    ),
    -- final cte
    final as (
        select
            orders.*,
            customers.first_name as customer_first_name,
            customers.last_name as customer_last_name,
            row_number() over (order by orders.order_id) as transaction_seq,
            row_number() over (partition by orders.customer_id order by orders.order_id) as customer_sales_seq,
            case when ( rank () over (partition by orders.customer_id 
                                        order by orders.order_placed_at, orders.order_id)) = 1 
                then 'new' 
                else 'return' 
            end as nvsr,
            sum (total_amount_paid) over (
                    partition by orders.customer_id 
                    order by orders.order_id 
                    range between unbounded preceding and current row
                ) as customer_lifetime_value,
            min(orders.order_placed_at) over (partition by orders.customer_id) as fdos
            from orders
            left join customers on orders.customer_id = customers.customer_id 
            order by order_id
    )
-- simple select statement
select * from final