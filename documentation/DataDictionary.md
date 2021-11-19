| fact_immigration | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| immigration_id | integer | primary key |
| citizenship_country_id | integer | immigrant country of citizenship |
| residency_country_id | integer | immigrant country of residency |
| arrival_city_id | integer | immigrant city of arrival in US |
| arrival_date | date | immigrant date of arrival in US |
| travel_mode_id | integer | immigrant method of travel to US |
| current_state_id | integer | US state where immigrant is living |
| departure_date | date | immigrant date of departure from US |
| immigration_age | integer | age of immigrant when arrived in US |
| visa_type_id | integer | type of visa immigrant used to enter US |
| birth_year | integer | immigrant year of birth |
| gender | char | immigrant gender |


| dim_date | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| date_id | date | primary key |
| month | integer | month (1 to 12) |
| day | integer | day of month |
| year | integer | year (4 digits) |
| week | integer | week of year (1 to 52) |
| day of week | integer | day of week starting Sunday (1 to 7) |


| dim_country | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| country_id | integer | primary key |
| country_code | integer | I-94 country code |
| country_name | string | name of country |


| dim_state | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| state_id | integer | primary key |
| state_code | string | state abbreviation |
| state_name | string | name of state |


| dim_city | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| city_id | integer | primary key |
| city_code | string | I-94 city code |
| city_name | string | name of city |
| state_id | integer | city's state |
| median_age | string | median age in city |
| male_population | string | male population in city |
| female_population | integer | female population in city |
| total_population | string | total population in city |
| veteran_population | string | veteran population in city |
| foreign_population | integer | foreign-born population in city |
| avg_household_size | string | avergae household size in city |
| race_majority | string | race majority in city |


| dim_visa_type | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| visa_type_id | integer | primary key |
| visa_type_description | string | visa description |


| dim_travel_mode | | |
---------------- | ----------- | -----------|
| Field Name | Field Type | Field Description |
| travel_mode_id | integer | primary key |
| travel_mode_desc | string | immigration travel mode description |
