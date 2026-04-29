with events as (
    select * from {{ ref('stg_events') }}
),

daily_patterns as (
    select
        cast(event_timestamp as date)           as event_date,
        content_flag,
        severity,
        count(*)                                as total_events,
        count(distinct user_id)                 as unique_users,
        sum(case when is_flagged then 1 else 0 end) as flagged_count

    from events
    group by
        cast(event_timestamp as date),
        content_flag,
        severity
)

select * from daily_patterns
order by event_date, flagged_count desc