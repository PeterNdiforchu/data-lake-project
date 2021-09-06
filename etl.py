import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import  year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Spark Session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: Process the song_data and make songs table and artists table.
    
        Parameters:
            spark = spark session
            input_data = path to song_data json file with metadata
            output_data = path to dimensional tables stored in parquet format
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + "/Songs/songs_table.parquet")

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "/Artists/artist_data.parquet")
    
    # create temporary table for songsplay table
    df.createOrReplaceTempView("song_df_table")
    

def process_log_data(spark, input_data, output_data):
    """
        Description: This function loads log_data from and to S3 by extracting both songs and artists tables, processing them and loading back to S3.
        
        Parameters:
            spark = spark session
            input_data = path to song_data json file with metadata
            output_data = path to dimensional tables stored in parquet format
            
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong').cache()

    # extract columns for users table    
    users_table =  df.select([df.userId.alias('user_id'), \
                              df.firstName.alias('first_name'), \
                              df.lastName.alias('last_name'), \
                              df.gender, \
                              df.level]).distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + "/Users/users_data.parquet")

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("timestamp", get_datetime(col("ts")))
    
    # create datetime column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%H:%M:%S'))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))
    
    time_table = df.select(col("start_time"), col("hour"),col("day"), \
    col("week"),col("month"),col("year"),col("weekday")).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "/Time/time_data.parquet")

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name, duration, title FROM song_df_table")
    df.createOrReplaceTempView('log_table')
    song_df.createOrReplaceTempView('song_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table = spark.sql("""select row_number() over (order by log_table.start_time) as songplay_id, \
                                                        log_table.start_time, year(log_table.start_time) year, \
                                                        month(log_table.start_time) month, log_table.userId as user_id, \
                                                        log_table.level, song_table.song_id, song_table.artist_id, \
                                                        log_table.sessionId as session_id, log_table.location, \
                                                        log_table.userAgent as user_agent \
                                                        from log_table \
                                                        join song_table on (log_table.artist = song_table.artist_name and \
                                                        log_table.song = song_table.title and log_table.length = song_table.duration )""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet("data/output_data/songplays.parquet")


def main():
    """
        The Main Function is responsible for calling 3 different functions to create a spark session, process the Song Data and to process the Log Data.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-s3datalakeproject"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
