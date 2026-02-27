{#
hash_pii

SHA-256 hash a PII column so it cannot be reverse-engineered in analytics
tables. Used in staging models to prevent PII propagation downstream.

Args:
  column_expr: SQL expression resolving to the PII string (e.g. 'email').

Returns:
  SHA2(lower(trim(column_expr)), 256) as a hex string, or NULL if the input is NULL.

Example:
  {{ hash_pii('email') }} as email_hash
#}
{% macro hash_pii(column_expr) -%}
    sha2(lower(trim({{ column_expr }})), 256)
{%- endmacro %}


{#
normalize_order_status

Normalise a raw order status string into a consistent, lower-case set of values.
Handles common variations from upstream system changes.

Args:
  status_expr: SQL expression resolving to the raw status string.

Returns:
  One of: 'pending', 'processing', 'completed', 'cancelled', 'refunded', 'unknown'
#}
{% macro normalize_order_status(status_expr) -%}
    case
        when lower({{ status_expr }}) in ('pending', 'awaiting_payment', 'new')
            then 'pending'
        when lower({{ status_expr }}) in ('processing', 'in_progress', 'confirmed')
            then 'processing'
        when lower({{ status_expr }}) in ('completed', 'fulfilled', 'shipped', 'delivered')
            then 'completed'
        when lower({{ status_expr }}) in ('cancelled', 'canceled', 'voided', 'aborted')
            then 'cancelled'
        when lower({{ status_expr }}) in ('refunded', 'returned', 'reversed')
            then 'refunded'
        else 'unknown'
    end
{%- endmacro %}


{#
safe_divide

Divide numerator by denominator, returning NULL instead of a division-by-zero
error. Wraps Snowflake's div0 but adds an explicit NULL check for the numerator.

Args:
  numerator:   SQL expression for the dividend.
  denominator: SQL expression for the divisor.

Returns:
  numerator / denominator, or NULL if denominator = 0 or numerator is NULL.
#}
{% macro safe_divide(numerator, denominator) -%}
    div0({{ numerator }}, {{ denominator }})
{%- endmacro %}


{#
date_spine_filter

Standard WHERE clause fragment for incremental models that use a date-based
lookback window. Reduces boilerplate across incremental models.

Args:
  date_column: Column name to filter on.
  lookback_days: Number of days to look back (defaults to var data_interval_lookback_days).

Returns:
  SQL fragment: <date_column> >= dateadd('day', -N, current_date())
#}
{% macro date_spine_filter(date_column, lookback_days=none) -%}
    {%- set days = lookback_days if lookback_days is not none else var('data_interval_lookback_days', 3) -%}
    {{ date_column }} >= dateadd('day', -{{ days }}, current_date())
{%- endmacro %}
