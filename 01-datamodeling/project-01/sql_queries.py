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
    FOREIGN KEY (user_id, level) REFERENCES users (user_id, level),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
    FOREIGN KEY ("timestamp") REFERENCES time ("timestamp")
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id VARCHAR NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR,
    level VARCHAR,
    UNIQUE (user_id, level),
    PRIMARY KEY (user_id, level)
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
# i'm using prefix "sa_" to represent staging area schema and corresponding
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
    YEAR INTEGER,
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
insert into songplay
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
select
    "timestamp",
    user_id ,
    song_id ,
    artist_id ,
    session_id ,
    level,
    location ,
    user_agent
from sa_songplay
on conflict ("timestamp", song_id, artist_id, user_id)
do nothing;
""")

user_table_insert = ("""
insert into users
(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
select
    user_id,
    first_name,
    last_name,
    gender,
    level
from sa_users
on conflict (user_id, level)
do nothing;
""")

song_table_insert = ("""
insert into songs
(
    song_id,
    artist_id,
    title,
    year,
    duration
)
values (%s, %s, %s, %s, %s)
on conflict (song_id)
do nothing;
""")

artist_table_insert = ("""
insert into artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
values (%s, %s, %s, %s, %s)
on conflict (artist_id)
do nothing;
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
select
    s.song_id,
    a.artist_id
from artists a,
    songs s
where 1=1
    and a.artist_id = s.artist_id
    and s.title ilike %s
    and a.name ilike %s
    and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [user_table_create, song_table_create,
                        artist_table_create, time_table_create,
                        songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

create_sa_table_queries = [songplay_table_create_sa, user_table_create_sa,
                           time_table_create_sa]

drop_sa_table_queries = [songplay_table_drop_sa, user_table_drop_sa,
                         time_table_drop_sa]
