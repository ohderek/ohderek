{#
stg_github__commit_files

Purpose: Thin staging wrapper over commit_file records.
         Classifies files into change types, detects lock files,
         noisy merge commits, and auto-generated files.

Grain: One row per (commit_sha, full_filepath).

Source: {{ source('github', 'commit_file') }}
        joined to {{ source('github', 'commit') }}
#}

with

commit_files as (

    select * from {{ source("github", "commit_file") }}
    where not coalesce(_fivetran_deleted, false)

),

commits as (

    select
        sha                                          as commit_sha,
        author_login,
        author_email,
        committed_date::timestamp_ntz                as committed_at,
        message,
        pull_request_id,
        _fivetran_synced::timestamp_ntz              as data_synced_at
    from {{ source("github", "commit") }}
    where not coalesce(_fivetran_deleted, false)

),

user_lookup as (

    select * from {{ source("identity", "user_lookup") }}

),

renamed as (

    select
        -- -------- ids
        cf.commit_sha,
        cf.pull_request_id,

        -- -------- file path decomposition
        cf.filename                                  as full_filepath,
        split_part(cf.filename, '/', -1)             as filename,
        iff(
            contains(cf.filename, '/'),
            regexp_replace(cf.filename, '/[^/]+$', ''),
            null
        )                                            as folder,

        -- -------- language / extension
        lower(split_part(cf.filename, '.', -1))      as file_extension,

        -- -------- change metrics
        coalesce(cf.additions, 0)::int               as additions,
        coalesce(cf.deletions, 0)::int               as deletions,
        coalesce(cf.changes, 0)::int                 as changes,
        cf.status                                    as change_type,

        -- -------- noise classification
        (
            lower(split_part(cf.filename, '.', -1)) in ('lock', 'sum')
            or cf.filename ilike '%package-lock.json%'
            or cf.filename ilike '%yarn.lock%'
            or cf.filename ilike '%uv.lock%'
            or cf.filename ilike '%Gemfile.lock%'
        )::boolean                                   as is_lock_file,

        -- -------- committer identity
        c.author_login                               as committer_login,
        coalesce(ul.ldap, c.author_login)            as committer_ldap,
        c.committed_at                               as committer_date,
        c.message,

        -- -------- PR merge commit detection
        (
            lower(c.message) like 'merge pull request%'
            or lower(c.message) like 'merge branch%'
        )::boolean                                   as is_pr_merge_commit,

        -- -------- audit
        cf._fivetran_synced::timestamp_ntz           as data_synced_at

    from commit_files cf
    left join commits c on cf.commit_sha = c.commit_sha
    left join user_lookup ul on lower(trim(c.author_login)) = lower(trim(ul.github_login))

),

final as (

    select
        -- -------- ids
        commit_sha,
        pull_request_id,
        {{ dbt_utils.generate_surrogate_key(['commit_sha', 'full_filepath']) }} as commit_file_pk,

        -- -------- file attributes
        full_filepath,
        folder,
        filename,
        file_extension,
        change_type,

        -- -------- metrics
        additions,
        deletions,
        changes,

        -- -------- flags
        is_lock_file,
        is_pr_merge_commit,
        (
            is_lock_file
            or is_pr_merge_commit
        )::boolean                                   as is_noisy_commit,

        -- -------- committer
        committer_login,
        committer_ldap,
        committer_date,
        message,

        -- -------- audit
        data_synced_at

    from renamed

)

select * from final
