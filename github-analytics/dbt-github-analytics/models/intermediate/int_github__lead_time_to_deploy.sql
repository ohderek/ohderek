{#
int_github__lead_time_to_deploy

Purpose: Match merged pull requests to their production deployments
         to compute lead time to deploy (a core DORA metric).

         Matching algorithm (cascade):
           1. Exact SHA match: merge_commit_sha = deployed_commit_sha
           2. Staging-to-prod chain: find the prod deployment that follows
              the first staging deployment containing the merge SHA
           3. No match: lead time is NULL (PR not yet deployed)

Grain: One row per (org, pr_number). A PR may have multiple deployment
       attempts but only the FIRST prod deployment is used.

Upstream:
  stg_github__pull_requests
  {{ source('deployments', 'deploy_events') }}

Downstream: fct_github__lead_time_to_deploy
#}

{{
    config(
        materialized         = "incremental",
        incremental_strategy = "merge",
        unique_key           = ["pr_id"],
        schema               = "intermediate",
        on_schema_change     = "append_new_columns",
    )
}}

with

pull_requests as (

    select
        pr_id,
        org,
        pr_number,
        author_ldap,
        merge_commit_sha,
        created_at,
        merged_at,
        pr_created_date,
        data_synced_at

    from {{ ref("stg_github__pull_requests") }}
    where is_merged

    {% if is_incremental() %}
        and data_synced_at >= (
            select dateadd('hour', -24, max(data_synced_at))
            from {{ this }}
        )
    {% endif %}

),

deployments as (

    select
        deploy_id,
        commit_sha,
        environment,
        service,
        deployed_at::timestamp_ntz  as deployed_at,
        deploy_source               as deployment_source
    from {{ source("deployments", "deploy_events") }}
    where environment in ('staging', 'production')

),

staging_deploys as (

    select
        commit_sha,
        min(deployed_at)            as first_staging_deploy_at

    from deployments
    where environment = 'staging'
    group by 1

),

prod_deploys as (

    select
        d.commit_sha,
        d.deployed_at               as first_prod_deploy_at,
        d.service,
        d.deployment_source         as prod_deployment_source,
        row_number() over (
            partition by d.commit_sha
            order by d.deployed_at
        )                           as _rn

    from deployments d
    where d.environment = 'production'

),

first_prod_deploys as (

    select * from prod_deploys where _rn = 1

),

-- Attempt 1: Exact SHA match (merge commit SHA = deployed commit SHA)
exact_match as (

    select
        pr.pr_id,
        pd.first_prod_deploy_at,
        pd.service,
        pd.prod_deployment_source,
        sd.first_staging_deploy_at,
        'exact_sha'                 as match_scenario

    from pull_requests pr
    join first_prod_deploys pd on pr.merge_commit_sha = pd.commit_sha
    left join staging_deploys sd on pr.merge_commit_sha = sd.commit_sha

),

-- Attempt 2: Staging-chain match (PR SHA in staging → first prod after staging)
staging_chain_match as (

    select
        pr.pr_id,
        min(pd.deployed_at)         as first_prod_deploy_at,
        min(pd.service)             as service,
        'staging_chain'             as match_scenario,
        sd.first_staging_deploy_at,
        null::varchar               as prod_deployment_source

    from pull_requests pr
    join staging_deploys sd on pr.merge_commit_sha = sd.commit_sha
    join prod_deploys pd on pd.deployed_at > sd.first_staging_deploy_at
    left join exact_match em on pr.pr_id = em.pr_id
    where em.pr_id is null    -- only use staging chain if no exact match
    group by 1, 4, 5

),

-- Combine match attempts
best_match as (

    select * from exact_match
    union all
    select * from staging_chain_match

),

enriched as (

    select
        pr.pr_id,
        pr.org,
        pr.pr_number,
        pr.service,
        pr.merge_commit_sha                                        as merge_commit_sha,
        pr.created_at                                              as pr_created_at,
        pr.merged_at,
        pr.pr_created_date,

        bm.first_staging_deploy_at,
        bm.first_prod_deploy_at,
        bm.match_scenario,
        bm.prod_deployment_source,
        bm.service                                                 as deployed_service,

        -- -------- lead time calculations
        datediff(
            'hour', pr.merged_at, bm.first_prod_deploy_at
        )                                                          as merge_to_prod_hours,

        datediff(
            'day', pr.merged_at, bm.first_prod_deploy_at
        )                                                          as merge_to_prod_days,

        datediff(
            'hour', pr.created_at, bm.first_prod_deploy_at
        )                                                          as lead_time_to_prod_hours,

        datediff(
            'day', pr.created_at, bm.first_prod_deploy_at
        )                                                          as lead_time_to_prod_days,

        datediff(
            'hour', pr.merged_at, bm.first_staging_deploy_at
        )                                                          as times_to_staging_hours,

        -- -------- audit
        pr.data_synced_at

    from pull_requests pr
    left join best_match bm on pr.pr_id = bm.pr_id

),

final as (

    select * from enriched

)

select * from final
