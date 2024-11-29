-- Problem 1 - DDL for actors table
create type film as (
	year integer,
	film text,
	votes integer,
	rating real,
	film_id text
);

create type quality as enum('star', 'good', 'average', 'bad');

create table actors (
	actor_name text,
	year integer,
	films film[],
	quality_class quality,
	is_active boolean,
	primary key(actor_name, year)
);