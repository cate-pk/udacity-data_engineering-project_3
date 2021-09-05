import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS sparkify_user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS start_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER, 
    last_name       VARCHAR,
    length          DECIMAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    DECIMAL,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR,
    user_id         VARCHAR
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs 
(
    num_songs        INTEGER,
    artist_id        VARCHAR, 
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         DECIMAL,
    year             INTEGER
)
""")

songplay_table_create = ("""
CREATE TABLE songplay 
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL, 
    user_id     VARCHAR NOT NULL,
    level       VARCHAR,
    song_id     VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    session_id  INTEGER,
    location    VARCHAR,
    user_agent  VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE sparkify_user 
(
    user_id    VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name  VARCHAR,
    gender     VARCHAR,
    level      VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE song 
(
    song_id   VARCHAR PRIMARY KEY,
    title     VARCHAR NOT NULL,
    artist_id VARCHAR,
    year      INTEGER,
    duration  DECIMAL NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artist 
(
    artist_id VARCHAR PRIMARY KEY,
    name      VARCHAR NOT NULL,
    location  VARCHAR,
    lattitude DECIMAL,
    longitude DECIMAL
)
""")

time_table_create = ("""
CREATE TABLE start_time 
(
    start_time TIMESTAMP PRIMARY KEY,
    hour       INTEGER,
    day        INTEGER,
    week       INTEGER,
    month      INTEGER,
    year       INTEGER,
    weekday    INTEGER
)
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {} 
iam_role {}
FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {} 
iam_role {}
FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time,
                     user_id,
                     level,
                     song_id,
                     artist_id,
                     session_id,
                     location,
                     user_agent
) 
SELECT 
    timestamp 'epoch' + (se.ts / 1000) * interval '1 second' AS start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events AS se
INNER JOIN staging_songs AS ss ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO sparkify_user(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT
    se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level
FROM staging_events AS se
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song(
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    CASE WHEN ss.year != 0 THEN ss.year ELSE null END AS year,
    ss.duration
FROM staging_songs AS ss
""")

artist_table_insert = ("""
INSERT INTO artist(
    artist_id,
    name,
    location,
    lattitude,
    longitude
)
SELECT 
    artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM (
  SELECT
      ss.artist_id,
      ss.artist_name,
      ss.artist_location,
      ss.artist_latitude,
      ss.artist_longitude,
      row_number() OVER (PARTITION BY ss.artist_id ORDER BY ss.year DESC)
  FROM staging_songs AS ss
)
WHERE row_number = 1
""")

time_table_insert = ("""
INSERT INTO start_time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT 
    start_time,
    extract(hour from start_time)  AS hour,
    extract(day from start_time)   AS day,
    extract(week from start_time)  AS week,
    extract(month from start_time) AS month,
    extract(year from start_time)  AS year,
    extract(dow from start_time)   AS weekday
FROM (
  SELECT 
  DISTINCT sp.start_time
  FROM songplay AS sp
)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]