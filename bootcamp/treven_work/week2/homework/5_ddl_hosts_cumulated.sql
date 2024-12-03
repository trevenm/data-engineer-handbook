create table hosts_cumulated (
    host text,
    host_activity_datelist date[],
    curr_date date,
    primary key (host, curr_date)
)
;