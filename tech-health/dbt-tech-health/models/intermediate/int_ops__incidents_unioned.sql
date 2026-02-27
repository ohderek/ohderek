{#
int_ops__incidents_unioned

Purpose: Union all three incident.io workspace staging models into a single
         normalised dataset. Computes TTD, TTR, TTS, impact_duration, and the
         Hot/Cold temperature classification.

         Uses Jinja looping over instances to avoid copy-paste — the same
         pattern used for multi-source CI pipelines in the platform domain.

Grain: One row per unique_ref.

Upstream:
  stg_incident_io__companya__incidents
  stg_incident_io__companyb__incidents
  stg_incident_io__companyc__incidents

Downstream: fct_ops__incidents
#}

{% set instances = [
    {"ref": "stg_incident_io__companya__incidents", "instance": "companya"},
    {"ref": "stg_incident_io__companyb__incidents", "instance": "companyb"},
    {"ref": "stg_incident_io__companyc__incidents", "instance": "companyc"},
] %}

{{
    config(
        materialized = "view",
        schema       = "intermediate",
    )
}}

with

{# -- import CTEs -- #}
{% for i in instances %}
stg_{{ i.instance }} as (
    select * from {{ ref(i.ref) }}
),
{% endfor %}

unioned as (

    {% for i in instances %}
    select
        unique_ref,
        instance,
        incident_id,
        reference,
        name,
        url,
        summary,
        postmortem_link,
        severity,
        creator_email,
        status,
        mode,
        is_internal_impact,
        impact_start_at,
        regressed_at,
        detected_at,
        reported_at,
        accepted_at,
        stabilized_at,
        resolved_at,
        closed_at,
        canceled_at,
        declined_at,
        merged_at,
        fixed_at,
        data_synced_at
    from stg_{{ i.instance }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

),

with_metrics as (

    select
        *,

        -- -------- time-to-detect (minutes: impact_start → detected_at)
        datediff(
            'minute', impact_start_at, detected_at
        )                                                                   as ttd,

        -- -------- time-to-report (minutes: impact_start → reported_at)
        greatest(0, datediff(
            'minute', impact_start_at, reported_at
        ))                                                                  as ttr,

        -- -------- time-to-stabilise (minutes: reported_at → stabilized_at)
        greatest(0, datediff(
            'minute', reported_at, stabilized_at
        ))                                                                  as tts,

        -- -------- impact duration (minutes: impact_start → stabilized_at)
        greatest(0, datediff(
            'minute', impact_start_at, stabilized_at
        ))                                                                  as impact_duration,

        -- -------- temperature: Hot = TTR ≤ 4 hours, Cold = TTR > 4 hours
        case
            when greatest(0, datediff('minute', impact_start_at, reported_at)) between 0 and 240
                then 'Hot'
            else 'Cold'
        end                                                                 as temperature,

        -- -------- derived dates
        date_trunc('day', impact_start_at)::date                           as incident_date,
        date_trunc('month', impact_start_at)::date                         as incident_month

    from unioned
    where unique_ref is not null

),

final as (

    select * from with_metrics

)

select * from final
