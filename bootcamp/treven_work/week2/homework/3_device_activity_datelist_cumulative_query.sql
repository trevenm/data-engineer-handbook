insert into user_devices_cumulated
with 
    yesterday as (
        select *
        from user_devices_cumulated
        where curr_date = date('2023-01-02')
    ),

    today as (
        select 
            cast(device_id as text) as device_id,
            date(cast(event_time as timestamp)) as date_active
        from events
        where date(cast(event_time as timestamp)) = date('2023-01-03')
        and device_id is not null
        group by 
            device_id,
            date(cast(event_time as timestamp))
    )

select 
    coalesce(t.device_id, y.device_id) as device_id,
    case
        when y.device_activity_datelist is null
            then array[t.date_active]
        when t.date_active is null
            then y.device_activity_datelist
        else array[t.date_active] || y.device_activity_datelist
    end as device_activity_datelist,
    coalesce(t.date_active,  y.curr_date + interval '1 day') as curr_date
from today t 
full outer join yesterday y on t.device_id = y.device_id
;




