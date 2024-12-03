insert into host_activity_reduced
with 
    daily_aggregate as (
        select
            host,
            date(event_time) as curr_date,
            count(1) as num_site_hits,
            count(distinct user_id) as unique_visitors
        from events
        where date(event_time) = date('2023-01-05')
        and host is not null
        group by host, date(event_time)
    ),

    yesterday_array as (
        select *
        from host_activity_reduced
        where month = date('2023-01-01')
    )

    

select 
    date(coalesce(ya.month, date_trunc('month',da.curr_date))) as month,    
    coalesce(da.host, ya.host) as host,    
    case
        when ya.hit_array is not null
            then ya.hit_array || array[coalesce(da.num_site_hits, 0)]
        when ya.hit_array is null
            then  array_fill(0, array[coalesce (curr_date - date(date_trunc('month', curr_date)), 0)]) 
                || array[coalesce(da.num_site_hits,0)]
    end as hit_array,
    case
        when ya.unique_visitors_array is not null
            then ya.unique_visitors_array  || array[coalesce(da.unique_visitors, 0)]
        when ya.unique_visitors_array  is null
            then  array_fill(0, array[coalesce (curr_date - date(date_trunc('month', curr_date)), 0)]) 
                || array[coalesce(da.unique_visitors,0)]
    end as unique_visitors_array
from daily_aggregate da 
full outer join yesterday_array ya on da.host = ya.host
on conflict (month, host)
do
    update set hit_array = excluded.hit_array,
        unique_visitors_array = excluded.unique_visitors_array
;







