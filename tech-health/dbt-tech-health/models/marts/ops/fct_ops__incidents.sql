{#
fct_ops__incidents

Purpose: Analyst-facing incident fact table. Thin wrapper over the
         intermediate union, enriched with dim_user for the incident
         lead's team attributes.

Grain: One row per unique_ref.

Upstream: int_ops__incidents_unioned
#}

{{
    config(
        materialized     = "incremental",
        incremental_strategy = "merge",
        unique_key       = "unique_ref",
        on_schema_change = "append_new_columns",
        schema           = "ops",
        persist_docs     = { "relation": true, "columns": true },
        snowflake_warehouse = (
            ("ETL__MEDIUM" if is_incremental() else "ETL__LARGE")
            if target.name == "production"
            else "ADHOC__" ~ env_var("WAREHOUSE", "MEDIUM")
        ),
    )
}}

with

incidents as (

    select * from {{ ref("int_ops__incidents_unioned") }}

    {% if is_incremental() %}
        where data_synced_at >= (
            select dateadd('hour', -2, max(data_synced_at))
            from {{ this }}
        )
    {% endif %}

),

final as (

    select
        -- -------- keys
        unique_ref,
        incident_id,
        reference,

        -- -------- dimensions
        instance,
        name,
        url,
        summary,
        postmortem_link,
        severity,
        creator_email,
        status,
        mode,

        -- -------- flags
        is_internal_impact,
        (postmortem_link is not null)::boolean as has_postmortem,

        -- -------- duration metrics (minutes)
        ttd,
        ttr,
        tts,
        impact_duration,

        -- -------- classification
        temperature,   -- Hot (TTR ≤ 4h) | Cold (TTR > 4h)

        -- -------- timestamps
        impact_start_at,
        detected_at,
        reported_at,
        stabilized_at,
        resolved_at,
        closed_at,
        fixed_at,
        canceled_at,

        -- -------- dates (for partitioning / filtering)
        incident_date,
        incident_month,

        -- -------- audit
        data_synced_at

    from incidents

)

select * from final
