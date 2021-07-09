# DROP TABLES

songplay_table_drop = "drop table if exists t_songplay;"
user_table_drop = "drop table if exists t_user;"
song_table_drop = "drop table if exists t_song;"
artist_table_drop = "drop table if exists t_artist;"
time_table_drop = "drop table if exists t_time;"

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
    user_agent varchar
)
""")

user_table_create = ("""
create table if not exists t_user 
( 
    user_id integer, 
    first_name varchar,
    last_name varchar,
    gender char,
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS 
t_song (
	song_id varchar, 
	title varchar,
	artist_id VARCHAR, 
	YEAR int, 
	duration float
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS t_artist 
(artist_id varchar, 
	name VARCHAR, 
	location varchar, 
	latitude float,
	longitude float);
""")

time_table_create = ("""
create table if not exists t_time 
("timestamp" timestamp,
	hour integer,
	day integer,
	weekofyear integer,
	month integer,
	year integer,
	weekday integer);
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
) values (%s,%s,%s,%s,%s,%s,%s,%s)
""")

user_table_insert = ("""
insert into t_user 
    (   user_id,
        first_name,
        last_name,
        gender,
        level
) values (%s, %s, %s, %s, %s);
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
values (%s, %s, %s, %s, %s);
""")

artist_table_insert = ("""
insert into t_artist (
    artist_id, 
	name, 
	location, 
	latitude,
	longitude) 
values (%s, %s, %s, %s, %s);
""")


time_table_insert = ("""
insert into t_time ("timestamp", hour, day, weekofyear, month, year, weekday)
values (%s, %s, %s, %s, %s, %s, %s);
""")

time_table_copy = ("""
COPY t_time
FROM '/home/eduardops/work/fbt/git/data-engineer-nanodegree/01-datamodeling/project-01/data/time_df.csv'
DELIMITER '|' CSV;
""")


user_table_copy = ("""
COPY t_user
FROM '/home/eduardops/work/fbt/git/data-engineer-nanodegree/01-datamodeling/project-01/data/user_df.csv'
DELIMITER '|' CSV;
""")

songplay_table_copy = ("""
COPY t_songplay
FROM '/home/eduardops/work/fbt/git/data-engineer-nanodegree/01-datamodeling/project-01/data/songplay_df.csv'
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

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]