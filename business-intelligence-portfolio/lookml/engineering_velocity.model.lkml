# engineering_velocity.model.lkml
# ─────────────────────────────────────────────────────────────────────────────
# Looker model for the Engineering Velocity subject area.
#
# Exposes two explores:
#   1. pr_velocity      — PR cycle time, review coverage, merge rates
#   2. dora_lead_time   — DORA lead time to deploy, deployment frequency
#
# All explores filter to human authors by default (bots excluded) and join
# the SCD2 user dimension for current-state team attribution.
# ─────────────────────────────────────────────────────────────────────────────

connection: "snowflake_github_insights"

# Include all views in this model
include: "/views/*.view.lkml"
include: "/dashboards/*.dashboard.lkml"

# ── Explore: PR Velocity ─────────────────────────────────────────────────────
explore: pr_velocity {
  label:       "PR Velocity"
  description: "Pull request cycle time, review coverage, and merge rates. Grain: one row per PR."
  view_name:   pr_facts

  # Always exclude bots from velocity metrics by default
  sql_always_where: ${pr_facts.author_type} = 'human' ;;

  join: dim_users {
    type:        left_outer
    sql_on:      ${pr_facts.author_ldap} = ${dim_users.ldap}
                 AND ${dim_users.is_current} ;;
    relationship: many_to_one
    view_label: "Author"
  }

  join: pr_review_summary {
    type:        left_outer
    sql_on:      ${pr_facts.pr_id} = ${pr_review_summary.pr_id} ;;
    relationship: one_to_one
    view_label: "Review"
  }
}


# ── Explore: DORA Lead Time ───────────────────────────────────────────────────
explore: dora_lead_time {
  label:       "DORA Lead Time to Deploy"
  description: "Lead time from first commit to production deployment, by service and team. Grain: one row per PR × service."
  view_name:   lead_time_to_deploy

  # Default to SHA-matched records only for high-confidence metrics
  # Analysts can remove this filter to include time-based matches
  sql_always_where: ${lead_time_to_deploy.prod_match_scenario} = 'sha_match' ;;

  join: pr_facts {
    type:        left_outer
    sql_on:      ${lead_time_to_deploy.pull_request_id} = ${pr_facts.pr_id} ;;
    relationship: many_to_one
    view_label: "Pull Request"
  }

  join: dim_users {
    type:        left_outer
    sql_on:      ${pr_facts.author_ldap} = ${dim_users.ldap}
                 AND ${dim_users.is_current} ;;
    relationship: many_to_one
    view_label: "Author"
  }
}
