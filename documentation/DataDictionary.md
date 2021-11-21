| fact_immigration | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| immigration_id | int8 | primary key |
| citizenship_country_id | int4 | immigrant country of citizenship |
| residency_country_id | int4 | immigrant country of residency |
| arrival_city_id | int4 | immigrant city of arrival in US |
| arrival_date | date | immigrant date of arrival in US |
| travel_mode_id | int4 | immigrant method of travel to US |
| current_state_id | int4 | US state where immigrant is living |
| departure_date | date | immigrant date of departure from US |
| immigration_age | int4 | age of immigrant when arrived in US |
| visa_type_id | int4 | type of visa immigrant used to enter US |
| birth_year | int4 | immigrant year of birth |
| gender | char(1) | immigrant gender |


| dim_date | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| date_id | date | primary key |
| month | int4 | month (1 to 12) |
| day | int4 | day of month |
| year | int4 | year (4 digits) |
| week | int4 | week of year (1 to 52) |
| day_of_week | int4 | day of week starting Sunday (1 to 7) |


| dim_country | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| country_id | int4 | primary key |
| country_code | int4 | I-94 country code |
| country_name | varchar(256) | name of country |


| dim_state | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| state_id | int4 | primary key |
| state_code | varchar(3) | state abbreviation |
| state_name | varchar(256) | name of state |


| dim_city | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| city_id | int8 | primary key |
| city_code | varchar(3) | I-94 city code |
| city_name | varchar(256) | name of city |
| state_id | int4 | city's state |
| median_age | numeric(3,1) | median age in city |
| male_population | int4 | male population in city |
| female_population | int4 | female population in city |
| total_population | int4 | total population in city |
| veteran_population | int4 | veteran population in city |
| foreign_population | int4 | foreign-born population in city |
| avg_household_size | numeric(2,2) | avergae household size in city |
| race_majority | string | race majority in city |


| dim_visa_type | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| visa_type_id | int4 | primary key |
| visa_type_desc | string | visa description |


| dim_travel_mode | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| travel_mode_id | int4 | primary key |
| travel_mode_desc | string | immigration travel mode description |
