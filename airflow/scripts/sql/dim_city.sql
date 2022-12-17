create table if not exists dim_city (
	id serial primary key,
	state_id int not null,
	city_name varchar,
	zip_code varchar,
	foreign key (state_id) references dim_state(id) on delete cascade
);

insert into dim_city (state_id, city_name, zip_code)
	select ds.id, c.office_city, c.office_zip_code  from companies c
	join dim_state ds on c.office_state_code  = ds.state_code
	where c.office_city is not null or ltrim(c.office_city) <> ''
		or c.office_zip_code is not null or ltrim(c.office_zip_code) <> ''
	group by ds.id, c.office_city, c.office_zip_code;