--1 
with 

-- 2 First CTE 
orders as(

    select * from {{ ref('stg_jaffle_shop__orders_v2') }}

),

payments as (

    select * from {{ ref('stg_stripe__payments_v2') }}
),

customers as (

    select * from {{ ref('stg_jaffle_shop__customers_v2') }}

),
--Staging

completed_payments as (

    select 
        order_id,
        max(payment_created_at) as payment_finalized_date,
        sum(payment_amount)  as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1

),

paid_orders as (
        select orders.order_id,
            orders.customer_id,
            orders.order_placed_at,
            orders.order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            c.first_name as customer_first_name,
            c.last_name as customer_last_name
        from  orders
            left join completed_payments as p on orders.id = p.order_id
    left join customers as c on orders.user_id = c.id
     ),

customer_orders as (
    select 
        c.id as customer_id
        , min(order_date) as first_order_date
        , max(order_date) as most_recent_order_date
        , count(orders.id) as number_of_orders
    from customers as c 
    left join orders on orders.user_id = c.id 
    group by 1
),

x as (

    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id

),

final as (

select
    p.*,

    --sale transaction sequence
    row_number() over (order by p.order_id) as transaction_seq,

    -- numéro sequence du clients
    row_number() over (partition by customer_id order by p.order_id) as customer_sales_seq,

    --new vs return
    case when c.first_order_date = p.order_placed_at
    then 'new'
    else 'return' end as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
from paid_orders p
left join customer_orders as c using (customer_id)
left outer join  x on x.order_id = p.order_id
order by order_id

)

select * from final