CREATE TABLE IF NOT EXISTS public.fact_immigration (
	immigration_id int8 NOT NULL,
	citizenship_country_id int4,
	residency_country_id int4,
	arrival_city_id int4,
	arrival_date date,
    	travel_mode_id int4,
    	current_state_id int4,
    	departure_date date,
    	immigration_age int4,
    	visa_type_id int4,
    	birth_year int4,
    	gender char(1),
    	CONSTRAINT fact_immigration_pkey PRIMARY KEY (immigration_id)
);

CREATE TABLE IF NOT EXISTS public.dim_date (
	date_id date NOT NULL,
	"month" int4,
    	"day" int4,
	"year" int4,
    	week int4,
	day_of_week int4,
	CONSTRAINT dim_date_pkey PRIMARY KEY (date_id)
);

CREATE TABLE IF NOT EXISTS public.dim_country (
	country_id int4 NOT NULL IDENTITY(1,1),
	country_code int4,
	country_name varchar(256),
	CONSTRAINT dim_country_pkey PRIMARY KEY (country_id)
);

CREATE TABLE IF NOT EXISTS public.dim_state (
	state_id int4 NOT NULL IDENTITY(1,1),
	state_code varchar(3),
	state_name varchar(256),
	CONSTRAINT dim_state_pkey PRIMARY KEY (state_id)
);

CREATE TABLE IF NOT EXISTS public.dim_city (
	city_id int4 NOT NULL IDENTITY(1,1),
    	city_code varchar(3),
	city_name varchar(256),
	state_id int4,
	median_age float,
	male_population int4,
	female_population int4,
	total_population int4,
	veteran_population int4,
	foreign_population int4,
	avg_household_size float,
	race_majority varchar(50),
	CONSTRAINT dim_city_pkey PRIMARY KEY (city_id)
);

CREATE TABLE IF NOT EXISTS public.dim_visa_type (
	visa_type_id int4 NOT NULL,
	visa_type_desc varchar(256),
	CONSTRAINT dim_visa_type_pkey PRIMARY KEY (visa_type_id)
);

CREATE TABLE IF NOT EXISTS public.dim_travel_mode (
	travel_mode_id int4 NOT NULL,
	travel_mode_desc varchar(256),
	CONSTRAINT dim_travel_mode_pkey PRIMARY KEY (travel_mode_id)
);

CREATE TABLE IF NOT EXISTS public.staging_immigration (
	immigration_id int8,
	citizenship_country_id int4,
	residency_country_id int4,
	arrival_city_id varchar(3),
	arrival_date date,
    	travel_mode_id int4,
    	current_state_id varchar(3),
    	departure_date date,
    	immigration_age int4,
    	visa_type_id int4,
    	birth_year int4,
    	gender char(1)
);

CREATE TABLE IF NOT EXISTS public.staging_demographic (
	city varchar(256),
	state_code varchar(3),
	median_age double precision,
	male_population int4,
	female_population int4,
	total_population int4,
	veteran_population int4,
	foreign_population int4,
	avg_household_size double precision,
	race_majority varchar(50),
    	race_count int4
);

CREATE TABLE IF NOT EXISTS public.staging_city (
    	city_code varchar(3),
	city_name varchar(256),
	state_code varchar(25)
);

CREATE TABLE IF NOT EXISTS public.staging_date (
    	date_id date
);





