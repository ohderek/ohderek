{#
stg_github__pull_requests

Purpose: Thin staging wrapper over the raw GitHub pull_request table.
         Normalises column names, casts types, and joins the identity
         lookup table to resolve GitHub login → LDAP.

Grain: One row per (org, number) — the natural PR key.

Source: {{ source('github', 'pull_request') }}
        left join {{ source('identity', 'user_lookup') }}
#}

with

pull_requests as (

    select * from {{ source("github", "pull_request") }}
    where not coalesce(_fivetran_deleted, false)

),

user_lookup as (

    select * from {{ source("identity", "user_lookup") }}

),

renamed as (

    select
        -- -------- ids
        pr.id                                                     as pr_id,
        pr.repo_id,
        pr.number                                                 as pr_number,

        -- -------- strings
        pr.org,
        pr.title,
        pr.url,
        pr.base_ref,
        pr.head_ref,
        pr.state,
        pr.user_login                                             as author_login,
        coalesce(ul.ldap, pr.user_login)                         as author_ldap,

        -- -------- booleans
        pr.draft::boolean                                         as is_draft,
        pr.merged::boolean                                        as is_merged,
        coalesce(pr.auto_merge, false)::boolean                   as is_auto_merge,

        -- -------- author classification
        case
            when pr.user_login ilike '%[bot]%'
              or pr.user_login ilike '%bot%'
              or ul.ldap is null              then 'bot'
            else 'human'
        end                                                       as author_type,

        -- -------- review signals
        pr.review_requested_by_owner_owl::boolean                 as review_requested_by_codeowner,

        -- -------- commit SHAs
        pr.merge_commit_sha,
        pr.base_sha,
        pr.head_sha,

        -- -------- timestamps
        pr.created_at::timestamp_ntz                              as created_at,
        pr.updated_at::timestamp_ntz                              as updated_at,
        pr.merged_at::timestamp_ntz                               as merged_at,
        pr.closed_at::timestamp_ntz                               as closed_at,

        -- -------- audit
        pr._fivetran_synced::timestamp_ntz                        as data_synced_at

    from pull_requests pr
    left join user_lookup ul
        on lower(trim(pr.user_login)) = lower(trim(ul.github_login))

),

final as (

    select
        -- -------- ids
        pr_id,
        repo_id,
        pr_number,

        -- -------- dimensions
        org,
        title,
        url,
        base_ref,
        head_ref,
        state,
        author_login,
        author_ldap,
        author_type,

        -- -------- booleans
        is_draft,
        is_merged,
        is_auto_merge,
        review_requested_by_codeowner,

        -- -------- derived
        (author_ldap = author_login)::boolean  as is_unresolved_identity,

        -- -------- SHAs
        merge_commit_sha,
        base_sha,
        head_sha,

        -- -------- dates
        created_at::date                       as pr_created_date,

        -- -------- timestamps
        created_at,
        updated_at,
        merged_at,
        closed_at,
        data_synced_at

    from renamed

)

select * from final
