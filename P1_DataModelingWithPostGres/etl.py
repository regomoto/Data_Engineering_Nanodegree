import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

"""
This function processes the song data JSON files It reads the JSON files 
into a pandas dataframe. It then enters data into the song_table and artist_table, 
only inserting the data fields we are interested in.
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.DataFrame([pd.read_json(filepath, typ = 'series')])

    # insert song record
    song_data = df.ix[0,['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
    song_data[3] = song_data[3].item()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.iloc[0,[1, 5, 4, 2, 3]].values.tolist()
    cur.execute(artist_table_insert, artist_data)

"""
This function processes the log data JSON files It reads the JSON files 
into a pandas dataframe. It then enters data into the time_table, user_table, 
and songplay_table, only inserting the data fields we are interested in.
"""
    
def process_log_file(cur, filepath):
    # open log file
    df = pd.DataFrame(pd.read_json(filepath, orient = 'columns',lines = True))

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.loc[:,'ts'], unit = 'ms')
    
    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict({column_labels[i]: time_data[i] for i in range(len(column_labels))})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

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
        songplay_data = (index, pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)

"""
This function processes all data files within in a specific location. 
It uses a function as input so it can use a function to process files in 
each iteration of the loop
"""
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

"""
Creates a connection with the database and runs the functions specified in this script.
It runs the process_data function, which processes all files in our data folder. It processes
the data using the process_song_file function to process each song data JSON file
and the process_log_file to process each activity log JSON file
"""
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()