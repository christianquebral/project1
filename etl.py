import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files


def process_song_file(cur, filepath):
    # open song file
    filepath = 'data/song_data'
    song_files = get_files(filepath)
    df = pd.DataFrame(pd.read_json(song_files[1], lines=True))

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = song_data[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = artist_data[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    filepath = 'data/log_data'
    log_files = get_files(filepath)
    df1 = pd.DataFrame(pd.read_json(log_files[1], lines = True))

    # filter by NextSong action
    df1 = df1[df1['page'] == 'NextSong']
    

    # convert timestamp column to datetime
    t = pd.to_datetime(df1['ts'], unit = 'ms')
    df1['ts'] = pd.to_datetime(df1['ts'], unit='ms')
    
    # insert time data records
    time_data = [t.dt.time, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.dayofweek]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_dict = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame(time_dict)
    

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df1[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    # df1 is the dataframe holding the Log_Data.
    for index, row in df1.iterrows():

    # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
