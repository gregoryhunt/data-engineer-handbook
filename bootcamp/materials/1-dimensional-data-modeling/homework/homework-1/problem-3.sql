-- Problem 3 - DDL for actors_history_scd
create table actors_history_scd (
	actor_name text,
	quality_class quality,
	is_active boolean,
	start_year integer,
	end_year integer,
	current_year integer,
	primary key(actor_name, start_year, end_year, current_year)

);

create type actor_scd_type as (
	quality_class quality,
	is_active boolean,
	start_year integer,
	end_year integer
)