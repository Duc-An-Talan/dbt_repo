--1 
with 

-- 2 First CTE 

customers as (

    select * from {{ ref('stg_jaffle_shop__customers_v2') }}

),
--Staging



paid_orders as (
        
        select * from  {{ ref('int_orders_v2') }}
            --c.first_name as customer_first_name,
            --c.last_name as customer_last_name
        
        --orders  left join completed_payments as p on orders.id = p.order_id
    --left join customers as c on orders.user_id = c.id
     ),

final as (
    select 
        paid_orders.order_id,
        paid_orders.customer_id,
        paid_orders.order_placed_at,
        paid_orders.order_status,
        paid_orders.total_amount_paid,
        paid_orders.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name,
            --sale transaction sequence
        row_number() over (order by paid_orders.order_id) as transaction_seq,
            -- numéro sequence du clients
        row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,

            --new vs return
        case  
        when (rank() 
        over (partition by paid_orders.customer_id
        order by paid_orders.order_placed_at, order_id
        ) = 1)
        then 'new'
        else 'return' end as nvsr,

            -- customer lifetime value
        sum(paid_orders.total_amount_paid) over (partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
        ) as customer_lifetime_value,

            -- first day of sale
         first_value(paid_orders.order_placed_at) over (partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
        ) as first_order_date,

        last_value(paid_orders.order_placed_at) over (partition by paid_orders.customer_id
        order by paid_orders.order_placed_at
        ) as most_recent_order_date,

        count(paid_orders.order_id) over ( partition by paid_orders.customer_id
        ) as number_of_orders
    from paid_orders 
    left join customers as c on paid_orders.customer_id = c.customer_id 
    
)


select * from final