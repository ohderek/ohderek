{#
fct_github__commit_files

Purpose: File-level commit fact table. One row per (commit_sha, filepath).
         Supports code churn analysis, file hotspot detection, language
         contribution tracking, and noisy-commit filtering.

Grain: One row per (commit_sha, full_filepath).

Upstream: stg_github__commit_files, dim_github__users, dim_github__repositories
#}

{{
    config(
        materialized         = "incremental",
        incremental_strategy = "merge",
        unique_key           = "commit_file_pk",
        cluster_by           = ["committer_date"],
        on_schema_change     = "append_new_columns",
        persist_docs         = { "relation": true, "columns": true },
    )
}}

with

commit_files as (

    select * from {{ ref("stg_github__commit_files") }}

    {% if is_incremental() %}
        where data_synced_at >= (
            select dateadd('day', -7, max(data_synced_at))
            from {{ this }}
        )
    {% endif %}

),

pull_requests as (

    select
        pr_id,
        org,
        repo_id,
        service

    from {{ ref("stg_github__pull_requests") }}

),

committers as (

    select ldap, core_lead, core_plus_1, function_l1_name, sup_org_name
    from {{ ref("dim_github__users") }}
    where is_current

),

final as (

    select
        -- -------- keys
        cf.commit_file_pk,
        cf.commit_sha,
        cf.pull_request_id,

        -- -------- PR / org context
        pr.org,
        pr.repo_id,
        pr.service,

        -- -------- file attributes
        cf.full_filepath,
        cf.folder,
        cf.filename,
        cf.file_extension,
        cf.change_type,

        -- -------- language (basic heuristic — extend as needed)
        case
            when cf.file_extension in ('py', 'pyi')         then 'Python'
            when cf.file_extension in ('ts', 'tsx')         then 'TypeScript'
            when cf.file_extension in ('js', 'jsx', 'mjs')  then 'JavaScript'
            when cf.file_extension in ('go')                then 'Go'
            when cf.file_extension in ('java', 'kt')        then 'JVM'
            when cf.file_extension in ('rb')                then 'Ruby'
            when cf.file_extension in ('rs')                then 'Rust'
            when cf.file_extension in ('sql')               then 'SQL'
            when cf.file_extension in ('swift')             then 'Swift'
            else 'Other'
        end                                                  as language,

        -- -------- change metrics
        cf.additions,
        cf.deletions,
        cf.changes,

        -- -------- noise flags
        cf.is_lock_file,
        cf.is_pr_merge_commit,
        cf.is_noisy_commit,

        -- -------- committer identity
        cf.committer_login,
        cf.committer_ldap,
        u.core_lead,
        u.core_plus_1,
        u.function_l1_name,
        u.sup_org_name,

        -- -------- timestamps
        cf.committer_date,
        cf.committer_date::date                              as commit_date,

        -- -------- audit
        cf.data_synced_at

    from commit_files cf
    left join pull_requests pr on cf.pull_request_id = pr.pr_id
    left join committers     u  on cf.committer_ldap  = u.ldap

)

select * from final
