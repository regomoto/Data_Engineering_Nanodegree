import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id
from pyspark.sql.types import TimestampType
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Get or create spark session using hadoop-aws package
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    
    This function:
    - Loads song data from S3 bucket 
    - Reads the song data into a dataframe
    - Extracts data to create the songs_table and artists_table
    - Writes the data back to S3 as parquet files
    
    Parameters:
    - spark: Spark session
    - input_data: location of the songs metadata in S3
    - output_data: location of S3 bucket where output data will be stored in parquet format
    
    """
    
    """
    Songs table:
    
    """
    
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'
    
    # read song data file. process data as a JSON file 
    song_df = spark.read.json(song_data)
    
    # extract columns to create songs table
    col_names = ['song_id', 'title', 'artist_id', 'year', 'duration']
    
    songs_table = song_df.select(col_names).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs')
    
    """
    Artists table:
    
    """
    
    # extract columns to create artists table
    col_names = ['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitutde']
    
    artists_table = song_df.selectExpr(*col_names).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists')
    
    
    
def process_log_data(spark, input_data, output_data):
    """
    This function reads in both the song data and log data. And outputs three tables
    to a S3 bucket. The location of the S3 bucket should be changed to a buckey you control. 
    
    Log data:
    - Loads log data from S3 bucket 
    - Reads the log data into a dataframe
    - Filters the data to get only columns with song play actions. 
      Do this by filtering the column page='nextSong'
    - Extract data to create the artists_table and artists_table
    - Writes the data back to S3 as parquet files
    
    Song data:
    - Loads song data from S3 bucket 
    - Reads the song data into a dataframe
    - Extract data to create the songplays_table
    - Writes the data back to S3 as parquet files
    
    Parameters:
    - spark: Spark session
    - input_data: location of the songs metadata in S3
    - output_data: location of S3 bucket where output data will be stored in parquet format
    
    """
    
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*/*.json'

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table    
    col = ['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level']
    
    users_table = log_df.selectExpr(*col).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), TimestampType())
    time_df = log_df.withColumn('start_time', to_timestamp('ts'))
    
    # extract columns to create time table
    time_table = time_df.select('start_time').dropDuplicates() \
    .withColumn('hour', hour('start_time')) \
    .withColumn('day', dayofmonth('start_time')) \
    .withColumn('week', weekofyear('start_time')) \
    .withColumn('month', month('start_time')) \
    .withColumn('year', year('start_time')) \
    .withColumn('weekday', date_format('start_time', 'EEEE'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time')

    # read in song data to use for songplays table
    song_data = input_data + 'song-data/*/*/*/*.json'
    song_df = spark.read.json(song_data)    
    
    # extract columns from joined song and log datasets to create songplays table 
    cols = ["songplay_id", "start_time", "userId as user_id", "level", "song_id", "artist_id", "sessionID as session_id", "location", "userAgent as user_agent"]
    
    songplays_table = songplays_df.selectExpr(*cols).\
        withColumn('year', year('start_time')).\
        withColumn('month', month('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays')


def main():
    spark = create_spark_session()
    input_data  = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-datalake-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
