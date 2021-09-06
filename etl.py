import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
import pandas as pd
import matplotlib
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = (spark.read
    .options("inferSchema", True)
    .json(song_data)
    .dropDuplicates()
    .cache()
    )

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table = output_data + "/songs_table.parquet"
    
    (songs_table.write
    .mode('overwrite')
    .partitionBy('year', 'artist_id')
    .parquet(songs_table)
    )

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
    
    # write artists table to parquet files
    artists_table = output_data + "/artist_data.parquet"
    
    (artist_table.write
    .mode("overwrite")
    .parquet(artists_table)
    )
    
    # create temporary table for songsplay table
    df.createOrReplaceTempView("song_df_table")
    

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log-data/*.json"

    # read log data file
    df = (spark.read
    .options("inferSchema", True)
    .json(log_data)
    .dropDuplicates()
    .cache()
    )
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong').cache()

    # extract columns for users table    
    users_table = df.select(["user_id", "firstName as first_name", "lastName as last_name", "gender", "level", "ts"]).distinct()
    
    # write users table to parquet files
    users_table = output_data + "/users_data.parquet"
    
    (users_table.write
    .mode("overwrite")
    .parquet(users_table)
    )

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : to_date(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(col("ts"))) 
    
    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", day("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))
    
    time_table = df.select(col("start_time"), col("hour"),col("day"), \
    col("week"),col("month"),col("year"),col("weekday")).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table = output_data + "/time_data.parquet"
    
    (time_table.write
    .partitionBy("year", "month")
    .mode("overwrite")
    .parquet(users_table)
    )

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, song_df.artist_name == df.artist , "inner") \
    .select(col("start_time"),col("userId"),col("level"),col("song_id") \
    ,col("artist_id"),col("sessionId"),col("artist_location"),col("userAgent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = output_data + "\songplays.parquet"
    
    (songplays_table.write
    .partitionBy("year", "month")
    .mode("overwrite")
    .parquet()
    )

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "data/output_data"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
