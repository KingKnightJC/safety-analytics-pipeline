with events as (
    select * from {{ ref('stg_events') }}
),

reports as (
    select * from {{ ref('stg_user_reports') }}
),

classifiers as (
    select * from {{ ref('stg_classifiers') }}
),

-- aggregate event level stats per user
event_stats as (
    select
        user_id,
        count(*)                                        as total_events,
        sum(case when is_flagged then 1 else 0 end)     as total_flagged_events,
        sum(case when severity = 'high' then 1 else 0 end)   as high_severity_count,
        sum(case when severity = 'medium' then 1 else 0 end) as medium_severity_count,
        min(event_timestamp)                            as first_seen_at,
        max(event_timestamp)                            as last_seen_at
    from events
    group by user_id
),

-- aggregate reports per user
report_stats as (
    select
        reported_user_id                                as user_id,
        count(*)                                        as total_reports,
        sum(case when is_active_report then 1 else 0 end) as active_reports
    from reports
    group by reported_user_id
),

-- get highest classifier score per user
classifier_stats as (
    select
        user_id,
        max(score)                                      as max_classifier_score,
        sum(case when is_flagged then 1 else 0 end)     as total_classifier_flags
    from classifiers
    group by user_id
),

-- join everything together
joined as (
    select
        e.user_id,
        e.total_events,
        e.total_flagged_events,
        e.high_severity_count,
        e.medium_severity_count,
        e.first_seen_at,
        e.last_seen_at,
        coalesce(r.total_reports, 0)            as total_reports,
        coalesce(r.active_reports, 0)           as active_reports,
        coalesce(c.max_classifier_score, 0)     as max_classifier_score,
        coalesce(c.total_classifier_flags, 0)   as total_classifier_flags
    from event_stats e
    left join report_stats r on e.user_id = r.user_id
    left join classifier_stats c on e.user_id = c.user_id
)

select * from joined