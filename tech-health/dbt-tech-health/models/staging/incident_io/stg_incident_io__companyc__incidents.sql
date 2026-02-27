{#
stg_incident_io__companyc__incidents

Purpose: Thin staging wrapper over the CompanyC incident.io workspace.
         CompanyC has no fixed_at or ttd available in source data.

Grain: One row per reference.
#}

with

incidents as (

    select * from {{ source("incident_io__companyc", "incident") }}
    where not coalesce(_fivetran_deleted, false)

),

times as (

    select * from {{ source("incident_io__companyc", "incident_times") }}
    where not coalesce(_fivetran_deleted, false)

),

renamed as (

    select
        -- -------- ids
        i.id                                                              as incident_id,
        i.reference,
        concat(regexp_substr(i.reference, '[0-9]+$', 1, 1), '-companyc') as unique_ref,
        'companyc'::varchar                                               as instance,

        -- -------- strings
        i.name,
        i.permalink                                                       as url,
        i.summary,
        i.postmortem_document_url                                         as postmortem_link,
        trim(substr(i.severity, 1, 4))                                   as severity,
        i.creator_email,
        i.status,
        i.mode,
        i.internal                                                        as is_internal_impact,

        -- -------- timestamps
        t.impact_start::timestamp_ntz                                     as impact_start_at,
        t.regressed_at::timestamp_ntz                                     as regressed_at,
        t.detected_at::timestamp_ntz                                      as detected_at,
        t.reported_at::timestamp_ntz                                      as reported_at,
        t.accepted_at::timestamp_ntz                                      as accepted_at,
        t.stabilized_at::timestamp_ntz                                    as stabilized_at,
        t.resolved_at::timestamp_ntz                                      as resolved_at,
        t.closed_at::timestamp_ntz                                        as closed_at,
        t.canceled_at::timestamp_ntz                                      as canceled_at,
        t.declined_at::timestamp_ntz                                      as declined_at,
        t.merged_at::timestamp_ntz                                        as merged_at,
        null::timestamp_ntz                                               as fixed_at,  -- not available for CompanyC

        -- -------- audit
        greatest(
            coalesce(i._fivetran_synced, '1900-01-01'),
            coalesce(t._fivetran_synced, '1900-01-01')
        )::timestamp_ntz                                                  as data_synced_at

    from incidents i
    left join times t on i.reference = t.reference

)

select * from renamed
