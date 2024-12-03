create table user_devices_cumulated (
    device_id text,
    device_activity_datelist date[],
    curr_date date,
    primary key (device_id, curr_date)
)
;
