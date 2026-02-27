{#
dim_github__users

Purpose: SCD Type 2 user dimension combining GitHub identity resolution
         with org hierarchy from the people system. Enables point-in-time
         attribution of PRs and commits to the engineer's team at the
         time of the event.

Grain: One row per (ldap, effective_start_date) — SCD Type 2.

Source: snp_people__users (snapshot of the people system)
#}

{{
    config(
        materialized = "table",
        alias        = "dim_user",
    )
}}

with

users as (

    select
        ldap,
        full_name,
        email,
        employee_id::varchar                              as employee_id,
        lead_name,
        lead_employee_id::varchar                         as lead_employee_id,
        core_lead,
        core_plus_1,
        core_plus_2,
        core_plus_3,
        core_plus_4,
        job_profile_set,
        job_family,
        job_family_group,
        sup_org_name,
        sup_org_id,
        sup_org_hierarchy,
        brand_id,
        brand_name,
        function_l1_id,
        function_l1_name,
        function_l2_id,
        function_l2_name,
        function_l3_id,
        function_l3_name,
        org_l1_id,
        org_l1_name,
        org_l2_id,
        org_l2_name,
        org_l3_id,
        org_l3_name,
        active_status,

        -- -------- SCD2 columns (from snapshot)
        dbt_valid_from::date                              as effective_start_date,
        nvl(dbt_valid_to::date, '9999-12-31'::date)       as effective_end_date,
        (dbt_valid_to is null)::boolean                   as is_current,
        dbt_scd_id                                        as user_pk,
        dbt_updated_at::timestamp_ntz                     as data_synced_at

    from {{ ref("snp_people__users") }}

),

final as (

    select * from users

)

select * from final
