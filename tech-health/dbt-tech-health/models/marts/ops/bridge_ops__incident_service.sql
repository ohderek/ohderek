{#
bridge_ops__incident_service

Purpose: Many-to-many bridge between incidents and the applications/services
         they impacted. Deduplicates originating_service values against the
         DIM_APPLICATION table using an upper-cased key match.

Grain: One row per (unique_ref, application_name) pair.

Upstream: {{ source('ops_raw', 'orig_service') }}, dim_ops__applications
#}

{{
    config(
        materialized = "incremental",
        incremental_strategy = "merge",
        unique_key   = ["unique_ref", "application_name"],
        schema       = "ops",
        persist_docs = { "relation": true, "columns": true },
    )
}}

with

orig_service as (

    select
        unique_ref,
        upper(trim(originating_service)) as app_key,
        _fivetran_synced::timestamp_ntz  as data_synced_at
    from {{ source("ops_raw", "orig_service") }}

),

applications as (

    select
        application_name,
        upper(trim(application_name)) as app_key,
        data_synced_at,
        row_number() over (
            partition by upper(trim(application_name))
            order by data_synced_at desc nulls last
        ) as _rn

    from {{ ref("dim_ops__applications") }}

),

deduped_apps as (

    select * from applications where _rn = 1

),

deduped_orig as (

    select
        unique_ref,
        app_key,
        max(data_synced_at) as inc_synced_at
    from orig_service
    group by 1, 2

),

joined as (

    select
        -- -------- keys
        o.unique_ref,
        a.application_name,

        -- -------- hashing
        concat(o.unique_ref, '|', a.application_name)         as pk,
        md5(coalesce(o.unique_ref, '') || '|' || coalesce(a.application_name, ''))
                                                              as hashdiff,

        -- -------- audit
        greatest(
            coalesce(o.inc_synced_at, '1900-01-01'),
            coalesce(a.data_synced_at, '1900-01-01')
        )::timestamp_ntz                                      as data_synced_at

    from deduped_orig o
    join deduped_apps a on o.app_key = a.app_key

    {% if is_incremental() %}
        where o.inc_synced_at >= (
            select dateadd('hour', -2, max(data_synced_at))
            from {{ this }}
        )
    {% endif %}

),

final as (

    select * from joined

)

select * from final
