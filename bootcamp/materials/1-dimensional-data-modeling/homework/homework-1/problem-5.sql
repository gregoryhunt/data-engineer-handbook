-- Problem 5 - Incremental query for actors_history_scd
with last_year_scd as (
	select * from actors_history_scd
	where current_year = 2020
	and end_year = 2020
),
historical_scd as (
	select
		actor_name,
		quality_class,
		is_active,
		start_year,
		end_year
	from actors_history_scd
	where current_year = 2020
	and end_year < 2020
),
this_year_data as (
	select * from actors
	where year = 2021
),
unchanged_records as (
	select ty.actor_name,
		   ty.quality_class,
		   ty.is_active,
		   ly.start_year,
		   ty.year as end_year
	from this_year_data ty
	join last_year_scd ly
	on ly.actor_name = ty.actor_name
	where ty.quality_class = ly.quality_class
	and ty.is_active = ly.is_active
),
changed_records as (
	select ty.actor_name,
		unnest(
		   array[
		   	row(
			   ly.quality_class,
			   ly.is_active,
			   ly.start_year,
			   ly.end_year
		   	)::actor_scd_type,
		   	 row(
			   ty.quality_class,
			   ty.is_active,
			   ty.year,
			   ty.year
		   	)::actor_scd_type
		   ]) as records
	from this_year_data ty
	left join last_year_scd ly
	on ly.actor_name = ty.actor_name
	where (ty.quality_class <> ly.quality_class
	or ty.is_active <> ly.is_active)
),
unnested_changed_records as (
	select actor_name,
	       (records::actor_scd_type).quality_class,
	       (records::actor_scd_type).is_active,
	       (records::actor_scd_type).start_year,
	       (records::actor_scd_type).end_year
	from changed_records
),
new_records as (
	select
	   ty.actor_name,
	   ty.quality_class,
	   ty.is_active,
	   ty.year as start_year,
	   ty.year as end_year
	from this_year_data ty
	left join last_year_scd ly
		on ty.actor_name = ly.actor_name
	where ly.actor_name is null
)
select * from historical_scd
union all
select * from unchanged_records
union all
select * from unnested_changed_records
union all
select * from new_records
