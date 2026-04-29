with source as (
    select * from {{ ref('raw_events') }}
),

cleaned as (
    select
        event_id,
        user_id,
        cast(timestamp as timestamp)    as event_timestamp,
        event_type,
        lower(content_flag)             as content_flag,
        session_id,

        -- flag whether this event had harmful content
        case
            when lower(content_flag) = 'none' then false
            else true
        end                             as is_flagged,

        -- categorise the severity
        case
            when lower(content_flag) in ('hate_speech', 'explicit_content') then 'high'
            when lower(content_flag) in ('harassment')                       then 'medium'
            when lower(content_flag) = 'none'                                then 'none'
            else 'low'
        end                             as severity

    from source
    where event_id is not null
)

select * from cleaned