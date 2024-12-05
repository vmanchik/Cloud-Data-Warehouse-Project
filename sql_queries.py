import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
    event_id           INT IDENTITY(0,1),
    artist_name        VARCHAR(100),
    auth               VARCHAR(50),
    user_first_name    VARCHAR(100),
    user_gender        CHAR(1),
    item_in_session    INTEGER,
    user_last_name     VARCHAR(100),
    song_length        DOUBLE PRECISION, 
    user_level         VARCHAR(20),
    location           VARCHAR(255),    
    method             VARCHAR(25),
    page               VARCHAR(35),    
    registration       BIGINT,    
    session_id         BIGINT,
    song_title         VARCHAR(100),
    status             INTEGER, 
    ts                 TIMESTAMP,
    user_agent         TEXT,    
    user_id            VARCHAR(50),
    PRIMARY KEY (event_id)
)
DISTSTYLE AUTO
SORTKEY(ts)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
    song_id            VARCHAR(100),
    num_songs          INTEGER,
    artist_id          VARCHAR(100),
    artist_latitude    DOUBLE PRECISION,
    artist_longitude   DOUBLE PRECISION,
    artist_location    VARCHAR(150),
    artist_name        VARCHAR(100),
    title              VARCHAR(150),
    duration           DOUBLE PRECISION,
    year               INTEGER,
    PRIMARY KEY (song_id)
)
DISTSTYLE AUTO
SORTKEY(artist_id)
""")

user_table_create = ("""
CREATE TABLE users(
    user_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    gender CHAR(1),
    level VARCHAR(10),
    PRIMARY KEY (user_id)
)
DISTSTYLE AUTO
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id VARCHAR(100) NOT NULL,
    title VARCHAR(150),
    artist_id VARCHAR(100) NOT NULL,
    year INTEGER,
    duration DOUBLE PRECISION,
    PRIMARY KEY (song_id)
)
DISTSTYLE AUTO
""")

artist_table_create = ("""
CREATE TABLE artists(
    artist_id VARCHAR(100) NOT NULL,
    name VARCHAR(150),
    location VARCHAR(150),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    PRIMARY KEY (artist_id)
)
DISTSTYLE AUTO
""")

songplay_table_create = ("""
CREATE TABLE songplays(
    songplay_id INT IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    level VARCHAR(10),
    song_id VARCHAR(100) NOT NULL,
    artist_id VARCHAR(100) NOT NULL,
    session_id BIGINT,
    location VARCHAR(255),
    user_agent TEXT,
    PRIMARY KEY (songplay_id)
)
DISTSTYLE AUTO
SORTKEY(start_time)

""")


time_table_create = ("""
CREATE TABLE time(
    start_time TIMESTAMP NOT NULL,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
)
DISTSTYLE AUTO 
SORTKEY(start_time)
""")


# STAGING TABLES

staging_events_copy = ("""
                       COPY staging_events FROM {}
                       CREDENTIALS 'aws_iam_role={}'
                       TIMEFORMAT as 'epochmillisecs'
                       TRUNCATECOLUMNS
                       BLANKSASNULL
                       EMPTYASNULL
                       JSON {}
                       """).format(LOG_DATA, IAM_ROLE, LOG_JSONPATH)

staging_songs_copy = ("""
                      COPY staging_songs FROM {}
                      CREDENTIALS 'aws_iam_role={}'
                      TRUNCATECOLUMNS
                      BLANKSASNULL
                      EMPTYASNULL
                      JSON 'auto'
                      """).format(SONG_DATA, IAM_ROLE)

# FINAL TABLES


user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT  
    user_id, 
    user_first_name, 
    user_last_name, 
    user_gender, 
    user_level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT 
    song_id, 
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
SELECT DISTINCT 
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT  
    e.ts as start_time, 
    e.user_id, 
    e.user_level, 
    s.song_id,
    s.artist_id, 
    e.session_id,
    e.location, 
    e.user_agent
FROM staging_events e
JOIN staging_songs s ON (e.song_title = s.title AND e.artist_name = s.artist_name )
WHERE e.page = 'NextSong' 
""")


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
SELECT start_time, 
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
FROM songplays 
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, user_table_create, song_table_create, artist_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,user_table_insert, song_table_insert, artist_table_insert, time_table_insert] 