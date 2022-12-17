create table if not exists dim_currency (
	id serial primary key,
	currency_name text,
	currency_code text not null
);

truncate table dim_currency restart identity cascade;

insert into dim_currency (currency_code) 
	select currency_code from topic_currency tc 
	group by tc.currency_code