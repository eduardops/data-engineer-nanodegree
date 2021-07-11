import os
import glob
import psycopg2
import pandas as pd
from sql_queries import (
    song_table_insert,
    artist_table_insert,
    time_table_copy,
    time_table_insert,
    user_table_copy,
    user_table_insert,
    song_select,
    songplay_table_copy,
    songplay_table_insert
)


def insert_song_data(df, cur):
    """
    - project corresponding song data from dataframe
    - and execute insert on dimension table song
    """
    # insert song record
    song_data = df[['song_id', 'artist_id', 'title',
                    'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)


def insert_artist_data(df, cur):
    """
    - project corresponding artist data from dataframe
    - and execute insert on dimension table artist
    """
    # insert artist record
    artist_columns = ['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude']
    artist_data = df[artist_columns].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_song_file(cur, filepath):
    """
    - Read from each one of the song files

    - extract song information
    - extract artist information

    - execute insert on corresponding dimension table
    """

    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    insert_song_data(df, cur)

    # insert artist record
    insert_artist_data(df, cur)


def stage_time_dimension_data(df, cur, output_file_path):
    """
    - transform time stamp data to create new columns
    - stores data in temporary file
    - execute sql copy to temporary stage table
    """

    # create new columns to time dimension
    time_data = (df['timestamp'],
                 df['timestamp'].dt.hour,
                 df['timestamp'].dt.day,
                 df['timestamp'].dt.isocalendar().week,
                 df['timestamp'].dt.month,
                 df['timestamp'].dt.year,
                 df['timestamp'].dt.weekday)

    column_labels = ('timestamp',
                     'hour',
                     'day',
                     'weekofyear',
                     'month',
                     'year',
                     'weekday')

    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    # write temporary file used in sql copy
    time_df.to_csv(output_file_path, index=False, header=False, sep='|')

    # copy users records
    cur.execute(time_table_copy)


def stage_user_dimension_data(df, cur, current_path):
    """
    - transform user data
    - stores data in temporary file
    - execute sql copy to temporary user stage table
    """
    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    output_file_path = f"{current_path}/data/user_df.csv"
    user_df.to_csv(output_file_path, index=False, header=False, sep='|')

    # copy users records
    cur.execute(user_table_copy)


def stage_songplay_data(df, cur, current_path):
    """
    - transform songplay data
    - stores data in temporary file
    - execute sql copy to temporary songplay stage table
    """
    songplay_data = []
    columns = ['timestamp', 'user_id', 'song_id', 'artist_id',
               'session_id', 'level', 'location', 'user_agent']

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data.append((row.timestamp,
                              row.userId,
                              songid,
                              artistid,
                              row.sessionId,
                              row.level,
                              row.location,
                              row.userAgent.replace("\"", "")
                              ))

    # create dataframe from songplay_data
    songplay_df = pd.DataFrame(songplay_data, columns=columns)

    # write corresponding file
    output_file_path = f"{current_path}/data/songplay_df.csv"
    songplay_df.to_csv(output_file_path, index=False, header=False, sep='|')

    # execute sql copy for songplay fact table
    cur.execute(songplay_table_copy)


def process_log_file(cur, filepath):
    """
    - extract and transform log file to create:
    - staging are for dimension table user data
    - staging are for dimension table time data
    - staging are for fact table songplay data
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['timestamp'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    current_path = os.path.dirname(os.path.abspath(__file__))
    temp_filepath = f"{current_path}/data/time_df.csv"

    # stage time data
    stage_time_dimension_data(df, cur, temp_filepath)

    # load user table
    stage_user_dimension_data(df, cur, current_path)

    stage_songplay_data(df, cur, current_path)


def process_data(cur, conn, filepath, func):
    """
    - iterate over all files in filepath mathing extension
    - and executes corresponding processing function
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def process_staging_area_to_tables(cur, conn):
    """
    - method responsible for populate dimensions time, dimension user
    - and fact table songplay from corresponding staging tables
    """
    print("processing stage to time table")
    cur.execute(time_table_insert)
    conn.commit()

    print("processing stage to user table")
    cur.execute(user_table_insert)
    conn.commit()

    print("processing stage to songplay table")
    cur.execute(songplay_table_insert)
    conn.commit()

    print("finished")


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    process_staging_area_to_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
