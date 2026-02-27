{#
fct_github__pull_requests

Purpose: Central PR fact table — canonical source for all PR velocity,
         review coverage, and DORA change frequency metrics.

         Joins pull request metadata with lifecycle timing and user
         dimension attributes for rich slicing.

Grain: One row per (org, pr_number).

Upstream:
  stg_github__pull_requests
  int_github__pr_lifecycle
  dim_github__users
  dim_github__repositories
#}

{{
    config(
        materialized         = "incremental",
        incremental_strategy = "merge",
        unique_key           = "pr_pk",
        cluster_by           = ["pr_created_date"],
        on_schema_change     = "append_new_columns",
        incremental_predicates = [
            "DBT_INTERNAL_DEST.pr_created_date >= dateadd(day, -7, current_date())"
        ],
        snowflake_warehouse = (
            ("ETL__LARGE" if is_incremental() else "ETL__XLARGE")
            if target.name == "production"
            else "ADHOC__" ~ env_var("WAREHOUSE", "MEDIUM")
        ),
        persist_docs = { "relation": true, "columns": true },
    )
}}

with

pull_requests as (

    select * from {{ ref("stg_github__pull_requests") }}

    {% if is_incremental() %}
        where data_synced_at >= (
            select dateadd('day', -7, max(data_synced_at))
            from {{ this }}
        )
    {% endif %}

),

lifecycle as (

    select * from {{ ref("int_github__pr_lifecycle") }}

),

authors as (

    select
        ldap,
        full_name,
        core_lead,
        core_plus_1,
        core_plus_2,
        core_plus_3,
        function_l1_name,
        function_l2_name,
        function_l3_name,
        sup_org_name,
        sup_org_id,
        sup_org_hierarchy,
        brand_name,
        job_profile_set,
        is_current

    from {{ ref("dim_github__users") }}
    where is_current

),

repos as (

    select
        repo_id,
        name                                                      as repo_name,
        full_name                                                 as repo_full_name,
        service,
        maturity,
        is_archived,
        is_private,
        pci,
        sox

    from {{ ref("dim_github__repositories") }}

),

final as (

    select
        -- -------- surrogate key
        {{ dbt_utils.generate_surrogate_key(['pr.org', 'pr.pr_number']) }} as pr_pk,

        -- -------- natural keys
        pr.pr_id,
        pr.org,
        pr.pr_number,
        pr.repo_id,

        -- -------- repo attributes
        r.repo_name,
        r.repo_full_name,
        r.service,
        r.maturity,
        r.pci,
        r.sox,

        -- -------- PR attributes
        pr.title,
        pr.url,
        pr.base_ref,
        pr.state,
        pr.author_login,
        pr.author_ldap,
        pr.author_type,

        -- -------- author team attributes (point-in-time via SCD2)
        u.full_name                                               as author_full_name,
        u.core_lead,
        u.core_plus_1,
        u.core_plus_2,
        u.core_plus_3,
        u.function_l1_name,
        u.function_l2_name,
        u.function_l3_name,
        u.sup_org_name,
        u.sup_org_id,
        u.sup_org_hierarchy,
        u.brand_name,
        u.job_profile_set,

        -- -------- PR booleans
        pr.is_draft,
        pr.is_merged,
        pr.is_auto_merge,
        pr.review_requested_by_codeowner,
        pr.is_unresolved_identity,

        -- -------- review metadata
        lc.reviewers_requested_count,
        lc.self_review,
        lc.total_reviews,
        lc.changes_requested_count,
        (lc.total_reviews > 0)::boolean                           as has_any_review,
        (lc.changes_requested_count > 0)::boolean                 as had_changes_requested,

        -- -------- lifecycle timestamps
        pr.created_at,
        pr.merged_at,
        pr.closed_at,
        lc.branch_created_at,
        lc.first_response_at,
        lc.first_reviewed_at,
        lc.first_approved_at,
        lc.last_approved_at,

        -- -------- epoch timings (seconds)
        lc.time_to_merge_epoch,
        lc.time_to_review_epoch,
        lc.time_to_approve_epoch,
        lc.time_to_first_response_epoch,
        lc.time_as_draft_epoch,

        -- -------- dates
        pr.pr_created_date,

        -- -------- SHA
        pr.merge_commit_sha,

        -- -------- audit
        pr.data_synced_at

    from pull_requests pr
    left join lifecycle     lc on pr.pr_id    = lc.pr_id
    left join authors       u  on pr.author_ldap = u.ldap
    left join repos         r  on pr.repo_id  = r.repo_id

)

select * from final
