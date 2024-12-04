-- Problem 2 - A DDL for an user_devices_cumulated table
-- a device_activity_datelist which tracks a users active days by browser_type
-- data type here should look similar to MAP<STRING, ARRAY[DATE]>
-- or you could have browser_type as a column with multiple rows for each user (either way works, just be consistent!)truncate user_devices_cumulated;

create table user_devices_cumulated (
    user_id numeric,
    browser_type text,
    date date,
    device_activity_datelist date[],
    primary key (user_id, browser_type, date)
)