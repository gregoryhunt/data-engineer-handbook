-- Problem 2 - Cumulative table generation query
insert into actors
	with yesterday as (
		select * from actors
		where year = 1969
	),
	today as (
		select  * from actor_films af 
		where year = 1970
	)
	select 
		coalesce(t.actor, y.actor_name) as actor_name,
		coalesce(t.year, y.year + 1) as year,
		case 
	        when y.films is null then 
	            array_agg(
	                row(t.year, t.film, t.votes, t.rating, t.filmid)::film
	            )
	        when t.year is not null then
	            y.films || array_agg(
	                row(t.year, t.film, t.votes, t.rating, t.filmid)::film
	            )
	        else y.films
		end as films,
		case 
			when t.year is not null then
				case
			        when avg(t.rating) > 8.0 then 'star'::quality
			        when avg(t.rating) > 7.0 and avg(t.rating) <= 8.0 then 'good'::quality
			        when avg(t.rating) > 6.0 and avg(t.rating) <= 7.0 then 'average'::quality
			        when avg(t.rating) <= 6.0 then 'bad'::quality
			    end
			else y.quality_class
		end as quality_class,
		case when t.year is not null then true else false end as is_active 
	from today t full outer join yesterday y
	on t.actor = y.actor_name
	group by
		coalesce(t.actor, y.actor_name),
		y.actor_name,
		y.films,
		t.year,
		y.year,
		y.quality_class;