{#
int_github__pr_lifecycle

Purpose: Compute full PR lifecycle timing metrics from raw event timestamps.
         Aggregates review and comment events from multiple raw tables to
         derive first_reviewed_at, first_approved_at, time_to_merge_epoch, etc.

         This intermediate model encapsulates all the business logic needed
         to produce the FACT_PR_TIMES mart table.

Grain: One row per (org, pr_number).

Upstream:
  stg_github__pull_requests
  stg_github__reviews
  stg_github__issue_comments

Downstream: fct_github__pr_times, fct_github__pull_requests
#}

{{
    config(
        materialized = "view",
        schema       = "intermediate",
    )
}}

with

pull_requests as (

    select * from {{ ref("stg_github__pull_requests") }}

),

reviews as (

    select * from {{ ref("stg_github__reviews") }}

),

first_events as (

    select
        pr_id,

        -- -------- first response = first comment OR first review (whichever comes first)
        min(submitted_at) filter (where event_type in ('review', 'comment')
                                     and not is_self_review
                                     and author_type = 'human')   as first_response_at,

        -- -------- first review (any state)
        min(submitted_at) filter (where event_type = 'review'
                                     and not is_self_review)      as first_reviewed_at,

        -- -------- first approval
        min(submitted_at) filter (where state = 'APPROVED'
                                     and not is_self_review)      as first_approved_at,

        -- -------- last approval (before merge)
        max(submitted_at) filter (where state = 'APPROVED')       as last_approved_at,

        -- -------- reviewer count
        count(distinct reviewer_ldap) filter (where event_type = 'review'
                                                 and not is_self_review)  as unique_reviewers,

        -- -------- self-review flag
        bool_or(is_self_review and event_type = 'review')         as has_self_review,

        -- -------- review counts
        count(*) filter (where event_type = 'review')             as total_reviews,
        count(*) filter (where state = 'CHANGES_REQUESTED')       as changes_requested_count

    from reviews
    group by pr_id

),

branch_created_at as (

    /*
    Branch creation time is estimated as:
      min(commit committed_at) across commits on this PR's head branch.
    This is a proxy — GitHub doesn't expose branch creation time directly
    via the REST API.
    */
    select
        cf.pull_request_id                                        as pr_id,
        min(cf.committer_date)                                    as branch_created_at

    from {{ ref("stg_github__commit_files") }} cf
    where not cf.is_pr_merge_commit
    group by 1

),

joined as (

    select
        -- -------- keys
        pr.pr_id,
        pr.org,
        pr.pr_number,

        -- -------- raw timestamps
        pr.created_at,
        pr.merged_at,
        pr.updated_at,
        bc.branch_created_at,

        -- -------- first event timestamps
        fe.first_response_at,
        fe.first_reviewed_at,
        fe.first_approved_at,
        fe.last_approved_at,

        -- -------- review metadata
        coalesce(fe.unique_reviewers, 0)                          as reviewers_requested_count,
        coalesce(fe.has_self_review, false)                       as self_review,
        coalesce(fe.total_reviews, 0)                             as total_reviews,
        coalesce(fe.changes_requested_count, 0)                   as changes_requested_count,

        -- -------- epoch timings (seconds)
        datediff(
            'second', pr.created_at, pr.merged_at
        )                                                         as time_to_merge_epoch,

        datediff(
            'second', pr.created_at, fe.first_reviewed_at
        )                                                         as time_to_review_epoch,

        datediff(
            'second', pr.created_at, fe.first_approved_at
        )                                                         as time_to_approve_epoch,

        datediff(
            'second', pr.created_at, fe.first_response_at
        )                                                         as time_to_first_response_epoch,

        -- -------- draft time (seconds in draft state)
        iff(
            pr.is_draft and pr.updated_at is not null,
            datediff('second', pr.created_at, pr.updated_at),
            null
        )                                                         as time_as_draft_epoch,

        -- -------- audit
        pr.data_synced_at

    from pull_requests pr
    left join first_events fe on pr.pr_id = fe.pr_id
    left join branch_created_at bc on pr.pr_id = bc.pr_id

),

final as (

    select * from joined

)

select * from final
