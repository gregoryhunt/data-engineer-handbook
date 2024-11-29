-- Problem 4 - Backfill query for actors_history_scd
insert into actors_history_scd
	with with_previous as (
		select
			actor_name,
			year,
			quality_class,
			is_active,
			lag(quality_class, 1) over (partition by actor_name order by year) as previous_quality_class,
			lag(is_active, 1) over (partition by actor_name order by year) as previous_is_active
		from actors
		where year <= 2020
	), with_indicators as (
		select *, 
			   case 
				   	when quality_class <> previous_quality_class then 1
				   	when is_active <> previous_is_active then 1 
				   	else 0
			   end as change_indicator	   
		from with_previous
	), with_streaks as (
		select *,
				sum(change_indicator) over (partition by actor_name order by year) as streak_identifier
		from with_indicators
	)
	select actor_name,
		   quality_class,
		   is_active,
		   min(year) as start_year,
		   max(year) as end_year,
		   2020 as current_year
	from with_streaks
	group by actor_name, streak_identifier, is_active, quality_class
	order by actor_name, streak_identifier