-- Problem 7 - A monthly, reduced fact table DDL host_activity_reduced
create table host_activity_reduced (
	host text,
	month_start date,
	hits real[],
	unique_visitors text[],
	primary key(host, month_start)
)