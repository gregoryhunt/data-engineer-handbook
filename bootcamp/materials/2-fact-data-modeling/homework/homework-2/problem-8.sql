-- Problem 8 - An incremental query that loads host_activity_reduced
-- - day-by-day

insert into host_activity_reduced
with daily_aggregate as (
	select
		host,
		cast(event_time as date) as date,
		count(1) as num_site_hits,
		count(distinct case when user_id is not null then user_id end) as num_distinct_users
	from events
	where cast(event_time as date) = '2023-01-03'
	group by host, cast(event_time as date)	
),
yesterday_array as (
	select * from host_activity_reduced
	where month_start = '2023-01-01'
)
select 
	coalesce(da.host, ya.host),
	coalesce(ya.month_start, date_trunc('month', da.date)) as month_start,
	case 
		when ya.hits is not null then
			ya.hits || array[coalesce(da.num_site_hits, 0)]
		when ya.hits is null then
			array_fill(0, array[coalesce(date - cast(date_trunc('month', date) as date), 0)]) || array[coalesce(da.num_site_hits, 0)]
	end as hits,
	case 
		when ya.unique_visitors is not null then
			ya.unique_visitors || array[coalesce(da.num_distinct_users, 0)]
		when ya.unique_visitors is null then
			array_fill(0, array[coalesce(date - cast(date_trunc('month', date) as date), 0)]) || array[coalesce(da.num_distinct_users, 0)]
	end as unique_visitors
from daily_aggregate da
full outer join yesterday_array ya
on da.host = ya.host
on conflict (host, month_start)
do
	update set hits = excluded.hits, unique_visitors = excluded.unique_visitors