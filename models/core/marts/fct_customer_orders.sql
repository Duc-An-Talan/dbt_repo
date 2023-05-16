-- with statement
with
-- import CTEs

--base_customers 
customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('int_orders') }}

    --select * from {{ ref('stg_jaffle_shop__orders') }} --{{ source('jaffle_shop', 'orders') }}

),
--mis dans int_orders.sql
/*payments as (

    select * from  {{ ref('stg_stripe__payments') }} --{{ source('stripe', 'payment') }}

),*/
-- logical CTEs
--staging
/*customers as (

    select 
        id as customer_id, --customers.
        --customers.name as full_name,
        last_name as surname, --customers.
        first_name as givenname, --customers.
        first_name || ' ' || last_name as full_name, 
        * 

    from base_customers

),

orders as (

      select 
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        row_number() over (
            partition by user_id 
            order by order_date, id
        ) as user_order_seq
        

      from base_orders

),

payments as (
    select 
    id as payment_id,
    orderid as order_id,
    status as payment_status,
    round(amount/100.0,2) as payment_amount

    from base_payments
    
),

/*b as ( 

    select 

        first_name || ' ' || last_name as name, 
        * 

    from customers

),*/


--marts
-- customer_order_history en final
customer_orders as (

    select 

        /*customers.customer_id,
        customers.full_name,
        customers.surname,
        customers.givenname,*/
        orders.*,
        customers.surname,
        customers.givenname,

        min(orders.order_date) over(
            partition by orders.customer_id
        ) as customer_first_order_date,

        min(valid_order_date) over(
            partition by orders.customer_id
        ) as customer_first_non_returned_order_date,

        max(valid_order_date) over(
            partition by orders.customer_id
        ) as customer_most_recent_non_returned_order_date,

        --coalesce(max(user_order_seq),0) 
        count(*) over(
            partition by orders.customer_id
        ) as customer_order_count,

        /*coalesce(count(case 
            when orders.order_status != 'returned' 
            then 1 end),
            0
        )*/
        -- nvl2 si null premiere ici 1 group by customer_id
        sum(nvl2(orders.valid_order_date, 1, 0)) over(
            partition by orders.customer_id
        ) 
         as customer_non_returned_order_count,
        -- nvl2 si null premiere ici amount group by customer_id
        /*sum(case 
            when orders.order_status not in ('returned','return_pending') 
            then payments.payment_amount 
            else 0 
        end)*/


        /*sum(case 
            when orders.order_status not in ('returned','return_pending') 
            then payments.payment_amount 
            else 0 
        end)
        / nullif(count(case 
            when orders.order_status not in ('returned','return_pending') 
            then 1 end),
            0
        )*/

        sum(nvl2(orders.valid_order_date, orders.order_value_dollars, 0)) over(
            partition by orders.customer_id
            ) as customer_total_lifetime_value,
        --montant moyen des objets non retourné
        --total_lifetime_value/non_returned_order_count
        -- as avg_non_returned_order_value,

        --donne la liste des orders_id par client
        array_agg(distinct orders.order_id) over(
            partition by orders.customer_id
        ) as order_ids

    from orders --a

    join customers --b
    on orders.customer_id =  customers.customer_id

    --left outer join payments --as c
    --on orders.order_id = payments.order_id

   -- where orders.order_status not in ('pending') --and payments.payment_status != 'fail'

    --group by customers.customer_id, customers.full_name, customers.surname, customers.givenname

),

--Une CTE intermediaire pour calculer avg_non_returned_value
add_avg_order_values as (

  select
    *,
    customer_total_lifetime_value / customer_non_returned_order_count 
    as customer_avg_non_returned_order_value

  from customer_orders
),

final as (

    select 
    order_id,
    customer_id,
    surname,
    givenname,
    customer_first_order_date as first_order_date,
    customer_order_count as order_count,
    customer_total_lifetime_value as total_lifetime_value,
    order_value_dollars,
    order_status,
    payment_status
    from add_avg_order_values
)

-- final CTE
/*final as (

    select 

        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        --payments.payment_amount as 
        orders.order_value_dollars,
        orders.order_status,
        --payments.payment_status
        orders.payment_status

    from orders

    join customers
    on orders.customer_id = customers.customer_id

    --join customer_order_history
    --on orders.customer_id = customer_order_history.customer_id

   -- left outer join payments
   -- on orders.order_id = payments.order_id

   -- where payments.payment_status != 'fail'

)*/


-- simple select statement
select * from final


/*select 
    orders.id as order_id,
    orders.user_id as customer_id,
    last_name as surname,
    first_name as givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    round(amount/100.0,2) as order_value_dollars,
    orders.status as order_status,
    payments.status as payment_status
from {{ source('jaffle_shop', 'orders') }}--raw.jaffle_shop.orders as orders

join /*(
      select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ source('jaffle_shop', 'orders') }}
)*/ /*customers
on orders.user_id = customers.id

join (

    select 
        b.id as customer_id,
        b.name as full_name,
        b.last_name as surname,
        b.first_name as givenname,
        min(order_date) as first_order_date,
        min(case when a.status NOT IN ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when a.status NOT IN ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        COALESCE(max(user_order_seq),0) as order_count,
        COALESCE(count(case when a.status != 'returned' then 1 end),0) as non_returned_order_count,
        sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end) as total_lifetime_value,
        sum(case when a.status NOT IN ('returned','return_pending') then ROUND(c.amount/100.0,2) else 0 end)/NULLIF(count(case when a.status NOT IN ('returned','return_pending') then 1 end),0) as avg_non_returned_order_value,
        array_agg(distinct a.id) as order_ids

    from /*(
      select 
        row_number() over (partition by user_id order by order_date, id) as user_order_seq,
        *
      from  {{ source('jaffle_shop', 'orders') }}--raw.jaffle_shop.orders
    )*/ --a

    --join
     /*( 
      select 
        first_name || ' ' || last_name as name, 
        * 
      from {{ source('jaffle_shop', 'orders') }}--raw.jaffle_shop.customers
    )*/ /*b
    on a.user_id = b.id

    left outer join {{ source('stripe', 'payment') }} c --raw.stripe.payment 
    on a.id = c.orderid

    where a.status NOT IN ('pending') and c.status != 'fail'

    group by b.id, b.name, b.last_name, b.first_name

) customer_order_history
on orders.user_id = customer_order_history.customer_id

left outer join  {{ source('stripe', 'payment') }} payments --raw.stripe.payment
on orders.id = payments.orderid

where payments.status != 'fail'*/