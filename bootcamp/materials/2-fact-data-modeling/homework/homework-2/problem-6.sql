-- Problem 6 - The incremental query to generate host_activity_datelist
insert into hosts_cumulated
with yesterday as (
	select
		*
	from hosts_cumulated
	where date = '2022-12-31'
),
today as (
	select
	e.host,
	cast(event_time as date) as date_active
	from events e
	where cast(e.event_time as date) = '2023-01-01'
	and e.user_id is not null
	group by e.host, cast(e.event_time as date)
)
select
coalesce(t.host, y.host) as host,
coalesce(t.date_active, y.date + interval '1 day') as date,
case when y.host_activity_datelist is null
	then array[t.date_active]
	when t.date_active is null then y.host_activity_datelist
	else y.host_activity_datelist || array[t.date_active]
end as dates_active
from today t
full outer join yesterday y
on t.host = y.host