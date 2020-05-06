#### Summary 
Create a PostgreSQL database that can be used for song play analysis. The database is created using song data and a log of user activitiy that are both in JSON format. 

NOTE: Files not available in this repository. Data was available in workspace provided by Udacity

#### How to run scripts
1) From the terminal, run create_tables.py (enter "python create_tables.py").

2) After successfully running file in the terminal, run the etl.py script (enter "python etl.py"). etl.py script should process 79 song_data files and 30 log_data files


#### Files in repo
1) create_tables.py()
Python script that creates the database that we will write all of our song and user activity log data (sparkify database). 
Creates all tables that are listed in the sql_queries.py file.

2) sql_queries.py()
Contains strings that will be used in SQL statements of the etl.py script. Strings specify the CREATE, INSERT INTO, DROP.
Also has a SQL query for getting the song_id and artist_id based on the song's title, artist name, and duration. This is needed for our the fact table in the star schema.

3) etl.py
Python script for processing song data and user activity data from the JSON files into the appropriate tables in the sparkify database. 
Uses CREATE and INSERT INTO statements from sql_queries.py(). Also creates dataframes for data processing that were tested in etl.ipynb

4) etl.ipynb
Notebook that was used to test all python code for etl script.

5) test.ipynb
test if the data was written to the database
