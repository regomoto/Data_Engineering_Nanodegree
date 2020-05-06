import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
#for data from log data files
staging_events_table_create= ("""
    CREATE TABLE staging_events
    (
        artist_name varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession INTEGER,
        lastName varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId varchar
    )
""")

#for data from song data files
staging_songs_table_create = ("""
    CREATE TABLE staging_songs
    (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplay
    (
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time timestamp NOT NULL,
        user_id varchar,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int,
        location varchar,
        user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE users
    (
        user_id varchar PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar NOT NULL 
    )
""")

song_table_create = ("""
    CREATE TABLE songs
    (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year int NOT NULL,
        duration float 
    )
""")

artist_table_create = ("""
    CREATE TABLE artists
    (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE time
    (
        start_time timestamp PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday varchar NOT NULL
    )
""")


# STAGING TABLES

staging_events_copy = ("""
        copy staging_events from {}
        credentials 'aws_iam_role=arn:aws:iam::783751936354:role/myRedshiftRole'
        json {}
        """).format(config.get('S3','LOG_DATA'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role=arn:aws:iam::783751936354:role/myRedshiftRole'
    region 'us-west-2' 
    format as JSON 'auto'
    timeformat as 'epochmillisecs';
""").format(config.get('S3','SONG_DATA'))



# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  (TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 Second ')  AS start_time, 
            e.userId AS user_id, 
            e.level AS level, 
            s.song_id AS song_id, 
            s.artist_id AS artist_id, 
            e.sessionId AS session_id, 
            e.location AS location, 
            e.userAgent AS user_agent
    FROM staging_events AS e
    JOIN staging_songs AS s ON (e.song = s.title) AND (e.artist_name = s.artist_name)
    WHERE e.page  =  'NextSong'
    ;
    """)

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT  DISTINCT(userId) AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  =  'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
 INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday
    FROM songplay;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

