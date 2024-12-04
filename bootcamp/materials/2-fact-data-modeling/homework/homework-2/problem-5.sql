-- Problem 5 - A DDL for hosts_cumulated table
-- - a host_activity_datelist which logs to see which dates each host is experiencing any activity
create table hosts_cumulated (
	host text,
    date date,
    host_activity_datelist date[],
    primary key (host, date)
)