with 
    devices as (
        select * 
        from user_devices_cumulated
        where curr_date = date('2023-01-03')
    ),

    series as (
        select * 
        from generate_series(date('2023-01-01'), date('2023-01-30'), interval '1 day')
            as series_date
    ),

    placeholder_ints as (
        select 
            case 
                when d.device_activity_datelist @> array[date(s.series_date)]
                    then cast(pow(2, 31 - (d.curr_date - date(s.series_date))) as bigint)
                else 0
            end as placeholder_int_value, 
            d.device_activity_datelist @> array[date(s.series_date)] as huh,
            *
        from devices d 
        cross join series s       
    )

select 
    device_id,
    cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as bits,
    bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_monthly_active,
    bit_count(cast('11111110000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))
        ) > 0 as dim_is_weekly_active,
    bit_count(cast('10000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))
        ) > 0 as dim_is_daily_active
from placeholder_ints
group by device_id
;

