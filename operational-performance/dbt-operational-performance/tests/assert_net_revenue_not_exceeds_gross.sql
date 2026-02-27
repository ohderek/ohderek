/*
Custom data test: assert_net_revenue_not_exceeds_gross

Validates that net_line_amount never exceeds gross_line_amount
across the fct_finance__revenue table (which would imply a negative discount).

This test runs on recent data only to avoid full-table scans in production.

Fails if: any row has net_line_amount > gross_line_amount
*/

select
    order_item_id,
    order_date,
    gross_line_amount,
    net_line_amount,
    discount
from {{ ref("fct_finance__revenue") }}
where
    order_date >= dateadd('day', -30, current_date())
    and net_line_amount > gross_line_amount
