create table if not exists dim_country (
	id serial primary key,
	country_code varchar not null
);

insert into dim_country (country_code)
	select office_country_code from companies
	group by office_country_code