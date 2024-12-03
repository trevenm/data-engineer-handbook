create table host_activity_reduced (
    month date,
    host text,
    hit_array int[],
    unique_visitors_array int[],
    primary key (month, host)
)
;