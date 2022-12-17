create table if not exists fact_currency_monthly_avg (
	id serial primary key,
	monthly_avg float8 not null,
	timestamp timestamp not null
);

insert into fact_currency_monthly_avg (monthly_avg, timestamp)
select avg(rate), current_timestamp from topic_currency tc 
	where tc."timestamp" > now() - interval '1 month'