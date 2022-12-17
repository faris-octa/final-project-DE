create table if not exists dim_state (
	id serial primary key,
	country_id int not null,
	state_code varchar not null,
	foreign key (country_id) references dim_country(id) on delete cascade
);

truncate table dim_state restart identity cascade;

insert into dim_state (country_id, state_code)
	select dc.id, c.office_state_code from companies c
	join dim_country dc on c.office_country_code = dc.country_code 
	where c.office_state_code is not null or ltrim(c.office_state_code) <> '' 
	group by dc.id, c.office_state_code;