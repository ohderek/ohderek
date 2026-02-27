/*
Custom data test: assert_orders_have_valid_customers

Validates referential integrity between orders and the customer dimension.
Orphaned orders (orders without a matching customer) indicate either a
pipeline ordering issue or a data quality problem in the source CRM.

Fails if: any order in the last 7 days has no matching customer record.
*/

select
    o.order_id,
    o.customer_id,
    o.order_date
from {{ ref("stg_ecommerce__orders") }} o
left join {{ ref("stg_ecommerce__customers") }} c
    on o.customer_id = c.customer_id
where
    o.order_date >= dateadd('day', -7, current_date())
    and c.customer_id is null
