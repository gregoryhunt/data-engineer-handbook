-- Problem 4 - A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column

with users as (
	select * from user_devices_cumulated
	where date = '2023-01-31'
),
series as (
	select * from generate_series(cast('2023-01-01' as date), cast('2023-01-31' as date), interval '1 day') as series_date
),
placeholder_ints as (
	select
		case 		
			when device_activity_datelist @> array[cast(series_date as date)]
			then cast(pow(2, 32 - (date - cast(series_date as date))) as bigint)
			else 0
		end as placeholder_int_value,
		*
	from users cross join series
)
select
	user_id,
	browser_type,
	cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as datelist_int
from placeholder_ints
group by user_id, browser_type