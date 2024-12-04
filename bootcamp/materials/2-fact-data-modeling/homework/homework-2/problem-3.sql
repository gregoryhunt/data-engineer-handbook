-- Problem 3 - A cumulative query to generate device_activity_datelist from events

insert into user_devices_cumulated
with yesterday as (
	select
		*
	from user_devices_cumulated
	where date = '2023-01-30'
),
today as (
	select
	e.user_id,
	d.browser_type,
	cast(event_time as date) as date_active
	from events e
	join devices d
	on d.device_id = e.device_id 
	where cast(e.event_time as date) = '2023-01-31'
	and e.user_id is not null
	group by e.user_id, d.browser_type, cast(e.event_time as date)
)
select
coalesce(t.user_id, y.user_id) as user_id,
coalesce(t.browser_type, y.browser_type) as browser_type,
coalesce(t.date_active, y.date + interval '1 day') as date,
case when y.device_activity_datelist is null
	then array[t.date_active]
	when t.date_active is null then y.device_activity_datelist
	else y.device_activity_datelist || array[t.date_active]
end as dates_active
from today t
full outer join yesterday y
on t.user_id = y.user_id and t.browser_type = y.browser_type
