class SqlQueries:
    fact_immigration_insert = ("""
        SELECT
                i.immigration_id,
                co1.country_id, 
                co2.country_id, 
                ci.city_id, 
                i.arrival_date, 
                i.travel_mode_id, 
                st.state_id, 
                i.departure_date, 
                i.immigration_age,
                i.visa_type_id,
                i.birth_year,
                i.gender
            FROM staging_immigration i
            LEFT JOIN dim_country co1 ON i.citizenship_country_id = co1.country_code
            LEFT JOIN dim_country co2 ON i.residency_country_id = co2.country_code
            LEFT JOIN dim_city ci ON i.arrival_city_id = ci.city_code
            LEFT JOIN dim_state st ON i.current_state_id = st.state_code
    """)
    
    dim_date_insert = ("""
        SELECT date_id, extract(month from date_id), extract(day from date_id), extract(year from date_id), extract(week from date_id), 
            extract(dayofweek from date_id)
        FROM staging_date
        WHERE date_id is not null
    """)

    dim_city_insert = ("""
        SELECT 
            c.city_code, 
            c.city_name,
            s.state_id,
            d.median_age,
            d.male_population,
            d.female_population,
            d.total_population,
            d.veteran_population,
            d.foreign_population,
            d.avg_household_size,
            d.race_majority
        FROM staging_city c
        LEFT JOIN dim_state s on c.state_code = s.state_code
        LEFT JOIN staging_demographic d on collate(c.city_name, 'case_insensitive') = collate(d.city, 'case_insensitive') and c.state_code = d.state_code 
    """)