
create table if not exists fact_city_per_state (
	id serial primary key,
	state varchar,
	total_city int not null,
	timestamp timestamp not null
);

insert into fact_city_per_state (total_city, timestamp)
	select count(office_city), office_state_code ,current_timestamp from companies c
		group by c.office_state_code 
		

create table if not exists fact_office_per_state (
	id serial primary key,
	state varchar,
	total_office int not null,
	timestamp timestamp not null
);

insert into fact_office_per_state (total_office, state, timestamp)
	select count(office_state_code), office_state_code, current_timestamp  from companies c
		group by c.office_state_code 