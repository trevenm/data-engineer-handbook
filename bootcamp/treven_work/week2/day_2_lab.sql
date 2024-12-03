-- drop table users_cumulated;
create table users_cumulated (
    user_id text,
    dates_active date[],
    curr_date date,
    primary key (user_id, curr_date)
)
;


insert into users_cumulated
with 
    yesterday as (
        select *
        from users_cumulated
        where curr_date = date('2023-01-30')
    ),

    today as (
        select 
            cast(user_id as text) as user_id,
            date(cast(event_time as timestamp)) as date_active
        from events
        where date(cast(event_time as timestamp)) = date('2023-01-31')
        and user_id is not null
        group by 
            user_id,
            date(cast(event_time as timestamp))
    )

select 
    coalesce(t.user_id, y.user_id) as user_id,
    case
        when y.dates_active is null
            then array[t.date_active]
        when t.date_active is null
            then y.dates_active
        else array[t.date_active] || y.dates_active
    end as dates_active,
    coalesce(t.date_active,  y.curr_date + interval '1 day') as curr_date
from today t 
full outer join yesterday y on t.user_id = y.user_id
;





with 
    users as (
        select * 
        from users_cumulated
        where curr_date = date('2023-01-31')
    ),

    series as (
        select * 
        from generate_series(date('2023-01-02'), date('2023-01-31'), interval '1 day')
            as series_date
    ),

    placeholder_ints as (
        select 
            case 
                when dates_active @> array[date(series_date)]
                    then cast(pow(2, 32 - (curr_date - date(series_date))) as bigint)
                else 0
            end as placeholder_int_value, 
            *
        from users u 
        cross join series s
        -- where user_id = '439578290726747300'
    )

select 
    user_id,
    cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as bits,
    bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_monthly_active,
    bit_count(cast('11111110000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))
        ) > 0 as dim_is_weekly_active,
    bit_count(cast('10000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))
        ) > 0 as dim_is_daily_active
from placeholder_ints
group by user_id
;


