{#
fct_github__lead_time_to_deploy

Purpose: DORA Lead Time to Deploy fact table. Analyst-facing thin wrapper
         over int_github__lead_time_to_deploy enriched with author dimensions.

Grain: One row per (org, pr_number).
#}

{{
    config(
        materialized         = "incremental",
        incremental_strategy = "merge",
        unique_key           = "pr_id",
        cluster_by           = ["pr_created_date"],
        on_schema_change     = "append_new_columns",
        persist_docs         = { "relation": true, "columns": true },
    )
}}

with

lead_time as (

    select * from {{ ref("int_github__lead_time_to_deploy") }}

    {% if is_incremental() %}
        where data_synced_at >= (
            select dateadd('day', -{{ var('lead_time_lookback_days') }}, max(data_synced_at))
            from {{ this }}
        )
    {% endif %}

),

authors as (

    select ldap, core_lead, core_plus_1, function_l1_name, sup_org_name, brand_name
    from {{ ref("dim_github__users") }}
    where is_current

),

final as (

    select
        -- -------- keys
        lt.pr_id,
        lt.pr_number,
        lt.org,

        -- -------- PR context
        lt.merge_commit_sha,
        lt.pr_created_at,
        lt.merged_at,
        lt.pr_created_date,

        -- -------- service / deployment context
        lt.deployed_service,
        lt.prod_deployment_source,

        -- -------- staging deploy
        lt.first_staging_deploy_at,
        lt.times_to_staging_hours,

        -- -------- prod deploy
        lt.first_prod_deploy_at,
        lt.merge_to_prod_hours,
        lt.merge_to_prod_days,
        lt.lead_time_to_prod_hours,
        lt.lead_time_to_prod_days,

        -- -------- match classification
        lt.match_scenario,
        (lt.first_prod_deploy_at is not null)::boolean   as is_deployed,

        -- -------- DORA bucket classification
        case
            when lt.lead_time_to_prod_hours <= 1     then 'elite'     -- < 1 hour
            when lt.lead_time_to_prod_hours <= 24    then 'high'      -- < 1 day
            when lt.lead_time_to_prod_days  <= 7     then 'medium'    -- < 1 week
            when lt.lead_time_to_prod_days  <= 180   then 'low'       -- < 6 months
            else null
        end                                              as dora_lead_time_bucket,

        -- -------- author team dimensions
        u.core_lead,
        u.core_plus_1,
        u.function_l1_name,
        u.sup_org_name,
        u.brand_name,

        -- -------- audit
        lt.data_synced_at

    from lead_time lt
    left join authors u on lt.author_ldap = u.ldap

)

select * from final
