
songs_table_query = """SELECT
                           song_id,
                           title,
                           artist_id,
                           year,
                           duration
                       FROM tmp
                       WHERE song_id IS NOT NULL
                    """
artists_table_query = """SELECT
                           song_id,
                           title,
                           artist_id,
                           year,
                           duration
                       FROM tmp
                       WHERE song_id IS NOT NULL
                    """
users_table_query = """SELECT
                           userId as user_id,
                           firstName as first_name,
                           lastName as last_name,
                           gender,
                           level
                       FROM tmp
                       WHERE userId IS NOT NULL
                   """
times_table_query = """SELECT
                       start_time,
                       hour,
                       day,
                       weekday,
                       week,
                       month,
                       year
                    FROM tmp 
                """
songplays_table_query = """SELECT
                               t.start_time,
                               t.user_id,
                               t.level,
                               t.sessionId as session_id,
                               t.location,
                               t.userAgent as user_agent,
                               t2.song_id,
                               t2.artist_id
                           FROM tmp t
                             INNER JOIN tmp2 t2 
                                   ON t.song=t2.title 
                                   AND t.length=t2.duration
                           WHERE t.user_id IS NOT NULL
                               AND t2.artist_id IS NOT NULL"""