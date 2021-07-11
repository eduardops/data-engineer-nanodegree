import os
# DROP TABLE

songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

songplay_table_drop_sa = "DROP TABLE IF EXISTS sa_songplay;"
user_table_drop_sa = "DROP TABLE IF EXISTS sa_users;"
# song_table_drop_sa = "DROP TABLE IF EXISTS sa_songs;"
# artist_table_drop_sa = "DROP TABLE IF EXISTS sa_artists;"
time_table_drop_sa = "DROP TABLE IF EXISTS sa_time;"

# CREATE TABLES
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    "timestamp" TIMESTAMP NOT NULL,
    user_id VARCHAR NOT NULL,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    level VARCHAR ,
    location VARCHAR,
    user_agent VARCHAR,
    UNIQUE ("timestamp", song_id, artist_id, user_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    FOREIGN KEY ("timestamp") REFERENCES time ("timestamp")
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id VARCHAR UNIQUE NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    level VARCHAR,
    PRIMARY KEY (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR UNIQUE NOT NULL,
    title VARCHAR,
    artist_id VARCHAR,
    YEAR int,
    duration FLOAT,
    PRIMARY KEY(song_id)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR UNIQUE NOT NULL,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    PRIMARY KEY(artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    "timestamp" timestamp UNIQUE,
    hour INTEGER,
    day INTEGER,
    weekofyear INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY ("timestamp")
);
""")


# NOTE: as might not be possible to create schemas in tests database
# i'm using prefix "sa_" to represent staging area schema AND corresponding
# tables

# sa tables
songplay_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_songplay
(
    "timestamp" timestamp,
    user_id VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    level VARCHAR,
    location VARCHAR,
    user_agent VARCHAR
);
""")

user_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_users
(
    user_id VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    level VARCHAR
);
""")

song_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_songs
(
    song_id VARCHAR,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT
);
""")

artist_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_artists
(
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
);
""")

time_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_time
(
    "timestamp" TIMESTAMP,
    hour INTEGER,
    day INTEGER,
    weekofyear INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplay
(
    "timestamp",
    user_id ,
    song_id ,
    artist_id ,
    session_id ,
    level,
    location ,
    user_agent
)
SELECT
    "timestamp",
    user_id ,
    song_id ,
    artist_id ,
    session_id ,
    level,
    location ,
    user_agent
FROM sa_songplay
ON CONFLICT ("timestamp", song_id, artist_id, user_id)
DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO users AS u
(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM sa_users
ON CONFLICT (user_id)
DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs
(
    song_id,
    artist_id,
    title,
    year,
    duration
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id)
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id)
DO NOTHING;
""")

time_table_insert = ("""
INSERT INTO time
      ("timestamp", hour, day, weekofyear, month, year, weekday)
SELECT "timestamp", hour, day, weekofyear, month, year, weekday
FROM sa_time
WHERE 1=1
ON CONFLICT ("timestamp")
DO NOTHING;
""")

current_path = os.path.dirname(os.path.abspath(__file__))

time_table_copy = (f"""
COPY sa_time
FROM '{current_path}/data/time_df.csv'
DELIMITER '|' CSV;
""")


user_table_copy = (f"""
COPY sa_users
FROM '{current_path}/data/user_df.csv'
DELIMITER '|' CSV;
""")

songplay_table_copy = (f"""
COPY sa_songplay
FROM '{current_path}/data/songplay_df.csv'
DELIMITER '|' CSV;
""")


# FIND song
song_select = ("""
SELECT
    s.song_id,
    a.artist_id
FROM artists a,
    songs s
WHERE 1=1
    AND a.artist_id = s.artist_id
    AND s.title ilike %s
    AND a.name ilike %s
    AND s.duration = %s
""")

remove_user_duplicates = (
    """
-- CREATE A TEMPORARY SEQUENCIAL VALUE TO EACH ROW
ALTER TABLE public.sa_users ADD temp_value SERIAL NOT NULL;

WITH cte AS (
	SELECT temp_value,
	user_id,
	ROW_NUMBER() OVER (
	        PARTITION BY
				user_id
			ORDER BY temp_value DESC
	    ) row_num
	FROM sa_users
)
DELETE FROM
sa_users su
WHERE su.temp_value IN (SELECT c.temp_value FROM cte c WHERE c.row_num > 1);
"""
)

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create,
                        artist_table_create, time_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

create_sa_table_queries = [songplay_table_create_sa, user_table_create_sa,
                           time_table_create_sa]

drop_sa_table_queries = [songplay_table_drop_sa,
                         user_table_drop_sa,
                         time_table_drop_sa]
