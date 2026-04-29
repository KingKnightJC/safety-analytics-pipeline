with source as (
    select * from {{ ref('raw_user_reports') }}
),

cleaned as (
    select
        report_id,
        reporter_user_id,
        reported_user_id,
        lower(reason)                   as reason,
        cast(timestamp as timestamp)    as reported_at,
        lower(status)                   as status,

        -- flag whether this report is still active
        case
            when lower(status) = 'dismissed' then false
            else true
        end                             as is_active_report

    from source
    where report_id is not null
)

select * from cleaned