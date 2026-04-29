with source as (
    select * from {{ ref('raw_classifiers') }}
),

cleaned as (
    select
        classifier_id,
        user_id,
        classifier_name,
        round(cast(score as decimal(4,2)), 2)       as score,
        round(cast(threshold as decimal(4,2)), 2)   as threshold,
        cast(timestamp as timestamp)                as classified_at,

        -- normalise the flagged field to a boolean
        flagged as is_flagged

    from source
    where classifier_id is not null
)

select * from cleaned