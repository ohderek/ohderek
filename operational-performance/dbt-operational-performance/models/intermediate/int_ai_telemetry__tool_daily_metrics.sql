{#
int_ai_telemetry__tool_daily_metrics

Purpose: Union all AI tool daily usage sources into a single normalised
         dataset. Computes a canonical total_tokens estimate using tool-
         specific logic (native tokens where available, otherwise estimated
         from request counts or lines of code).

         Jinja loops over a tool config list — same pattern as the CI
         Buildkite multi-schema union in the platform domain.

Grain: One row per (email, date, tool).

Upstream:
  stg_ai_telemetry__g2_user_activity_daily
  stg_ai_telemetry__goose_endpoint_usage
  stg_ai_telemetry__claude_code_usage
  stg_ai_telemetry__cursor_usage
  stg_ai_telemetry__chatgpt_usage
  stg_ai_telemetry__firebender_usage
  stg_ai_telemetry__amp_usage

Downstream: fct_ai_telemetry__tool_daily_metrics
#}

{{
    config(
        materialized         = "incremental",
        incremental_strategy = "merge",
        unique_key           = ["id"],
        on_schema_change     = "append_new_columns",
        schema               = "intermediate",
    )
}}

{% set tools = [
    "stg_ai_telemetry__g2_user_activity_daily",
    "stg_ai_telemetry__goose_endpoint_usage",
    "stg_ai_telemetry__claude_code_usage",
    "stg_ai_telemetry__cursor_usage",
    "stg_ai_telemetry__chatgpt_usage",
    "stg_ai_telemetry__firebender_usage",
    "stg_ai_telemetry__amp_usage",
] %}

with

{# -- import CTEs -- #}
{% for t in tools %}
{{ t }} as (
    select * from {{ ref(t) }}
    {% if is_incremental() %}
        where date >= (
            select dateadd('day', -{{ var('data_interval_lookback_days') }}, max(date))
            from {{ this }}
        )
    {% endif %}
),
{% endfor %}

unioned as (

    {% for t in tools %}
    select
        email,
        date,
        tool,
        is_active,
        total_tokens,
        metrics
    from {{ t }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

with_id as (

    select
        -- -------- surrogate key
        {{ dbt_utils.generate_surrogate_key(['email', 'date', 'tool']) }} as id,

        -- -------- ids
        email,

        -- -------- attributes
        date,
        tool,
        is_active,
        total_tokens,
        metrics

    from unioned
    where
        email is not null
        and date is not null
        and date >= '2025-07-01'
        and date <= current_date()

),

final as (

    select * from with_id

)

select * from final
