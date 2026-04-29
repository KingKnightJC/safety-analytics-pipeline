with signals as (
    select * from {{ ref('fct_user_safety_signals') }}
),

risk_tiered as (
    select
        user_id,
        total_events,
        total_flagged_events,
        high_severity_count,
        total_reports,
        active_reports,
        max_classifier_score,
        total_classifier_flags,
        first_seen_at,
        last_seen_at,

        -- calculate a simple risk score out of 100
        least(
            (total_flagged_events * 10)
            + (high_severity_count * 15)
            + (active_reports * 20)
            + (cast(max_classifier_score * 100 as integer) / 4)
        , 100)                              as risk_score,

        -- assign risk tier based on signals
        case
            when active_reports >= 2
                or high_severity_count >= 3
                or max_classifier_score >= 0.90  then 'high'
            when active_reports >= 1
                or high_severity_count >= 1
                or max_classifier_score >= 0.75  then 'medium'
            else 'low'
        end                                 as risk_tier

    from signals
)

select * from risk_tiered