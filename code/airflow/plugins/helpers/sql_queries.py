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

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)