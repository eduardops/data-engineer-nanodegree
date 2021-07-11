import os
# DROP TABLES

songplay_table_drop = "drop table if exists t_songplay;"
user_table_drop = "drop table if exists t_user;"
song_table_drop = "drop table if exists t_song;"
artist_table_drop = "drop table if exists t_artist;"
time_table_drop = "drop table if exists t_time;"

songplay_table_drop_sa = "drop table if exists sa_t_songplay;"
user_table_drop_sa = "drop table if exists sa_t_user;"
song_table_drop_sa = "drop table if exists sa_t_song;"
artist_table_drop_sa = "drop table if exists sa_t_artist;"
time_table_drop_sa = "drop table if exists sa_t_time;"

# CREATE TABLES
songplay_table_create = ("""
create table if not exists t_songplay
(
    "timestamp" timestamp,
    user_id varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    level varchar,
    location varchar,
    user_agent varchar,
    UNIQUE ("timestamp", song_id, artist_id, user_id)
);
""")

user_table_create = ("""
create table if not exists t_user
(
    user_id varchar unique,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar,
    primary key (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS t_song
(
    song_id varchar unique,
    title varchar,
    artist_id VARCHAR,
    YEAR int,
    duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS t_artist
(
    artist_id varchar unique,
    name VARCHAR,
    location varchar,
    latitude float,
    longitude float
);
""")

time_table_create = ("""
create table if not exists t_time
(
    "timestamp" timestamp unique,
    hour integer,
    day integer,
    weekofyear integer,
    month integer,
    year integer,
    weekday integer
);
""")


# NOTE: as might not be possible to create schemas in tests database
# i'm using prefix "sa_" to represent staging area schema and corresponding
# tables

# sa tables
songplay_table_create_sa = ("""
create table if not exists sa_t_songplay
(
    "timestamp" timestamp,
    user_id varchar,
    song_id varchar,
    artist_id varchar,
    session_id integer,
    level varchar,
    location varchar,
    user_agent varchar
);
""")

user_table_create_sa = ("""
create table if not exists sa_t_user
(
    user_id varchar,
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
);
""")

song_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_t_song
(
    song_id varchar,
    title varchar,
    artist_id VARCHAR,
    YEAR int,
    duration float
);
""")

artist_table_create_sa = ("""
CREATE TABLE IF NOT EXISTS sa_t_artist
(
    artist_id varchar,
    name VARCHAR,
    location varchar,
    latitude float,
    longitude float
);
""")

time_table_create_sa = ("""
create table if not exists sa_t_time
(
    "timestamp" timestamp,
    hour integer,
    day integer,
    weekofyear integer,
    month integer,
    year integer,
    weekday integer
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
insert into t_songplay
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
from sa_t_songplay
on conflict ("timestamp", song_id, artist_id, user_id)
do nothing;
""")

user_table_insert = ("""
insert into t_user
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
from sa_t_user
on conflict (user_id)
do nothing;
""")

song_table_insert = ("""
insert into t_song
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
insert into t_artist (
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
INSERT INTO t_time
      ("timestamp", hour, day, weekofyear, month, year, weekday)
SELECT "timestamp", hour, day, weekofyear, month, year, weekday
FROM sa_t_time
WHERE 1=1
ON CONFLICT ("timestamp")
DO NOTHING;
""")

current_path = os.path.dirname(os.path.abspath(__file__))

time_table_copy = (f"""
COPY sa_t_time
FROM '{current_path}/data/time_df.csv'
DELIMITER '|' CSV;
""")


user_table_copy = (f"""
COPY sa_t_user
FROM '{current_path}/data/user_df.csv'
DELIMITER '|' CSV;
""")

songplay_table_copy = (f"""
COPY sa_t_songplay
FROM '{current_path}/data/songplay_df.csv'
DELIMITER '|' CSV;
""")


# FIND SONGS
song_select = ("""
select
    s.song_id,
    a.artist_id
from t_artist a,
    t_song s
where 1=1
    and a.artist_id = s.artist_id
    and s.title ilike %s
    and a.name ilike %s
    and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create,
                        song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]

create_sa_table_queries = [songplay_table_create_sa, user_table_create_sa,
                           song_table_create_sa, artist_table_create_sa,
                           time_table_create_sa]
drop_sa_table_queries = [songplay_table_drop_sa, user_table_drop_sa,
                         song_table_drop_sa, artist_table_drop_sa,
                         time_table_drop_sa]
