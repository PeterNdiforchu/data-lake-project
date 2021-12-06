# Data Lake Project
>The goal of this project is to load data from S3, process the data into analytics tables using Spark, and load them back into S3 
>as a set of dimensional tables in order to allow >the analytics team to draw insights in what songs users are listening to.

## Table of Contents
* [General info](#general-info)
* [Screenshots](#screenshots)
* [Technology and Libraries](#technology-and-libraries)
* [Project Setup](#project-setup)
* [Contact](#contact)

## General info
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. To better serve customers the start-up wants to better understand how users interact with the app further analyis needs to be performed on the data to compare results with expected results.

## Screenshots
Fig.1: `output_data` showing `Artists/`, `Songs/`, `Time/` and `Users/` path
![project-datalake-img1](https://user-images.githubusercontent.com/76578061/132271586-575b1511-c80b-4696-a9a4-770c91c44bf7.png)

Fig.2: Sample `artist_data.parquet/` data loaded back into S3
![project-datalake-img2](https://user-images.githubusercontent.com/76578061/132271624-dcebd7ac-ae54-4bb4-b5f1-c327b2f87adb.png)

Fig.3: Sample `songs_table.parquet/` data loaded back into S3
![project-datalake-img3](https://user-images.githubusercontent.com/76578061/132271661-78894259-d4c9-4854-a972-bd7e258a5061.png)

Fig.4: Sample `time_data.parquet/` partitioned by `year` and `month` loaded into S3
![project-datalake-img4](https://user-images.githubusercontent.com/76578061/132271703-bf77fde2-80b9-464c-a162-b6dc7882f0a5.png)

Fig.5: Sample `users_data.parquet/` data loaded back into S3
![project-datalake-img5](https://user-images.githubusercontent.com/76578061/132271732-6e8bb465-b161-425b-8178-fc6901e1f34e.png)

## Technology and Libraries
* Jupyter - version 1.0.9
* configparser
* pyspark
* datetime

## Project Setup
In this section, we cover the different project scripts and the database schema for song play analyis. 

The project is made up of an `etl.py` script for initializing the spark cluster, loading the data from S3, processing data into dimensional model and loading back to S3. The `dl.cfg` is stores the configuration parameters for reading and writing to S3. 

Using the song and log datasets, a star schema optimized for queries on song play analysis derived. The Fact table for this schema model was made of a single table - `songplays` - which records in log data associated with song plays .i.e. records with page `NextSong`. The query below applies a condition that filters actions for song plays:

`df = df.where(df.page == 'NextSong')`

The query below extracts columns from both song and log dataset to create `songplays` table

`songplays_table = spark.sql("""select row_number() over (order by log_table.start_time) as songplay_id, \
                                                        log_table.start_time, year(log_table.start_time) year, \
                                                        month(log_table.start_time) month, log_table.userId as user_id, \
                                                        log_table.level, song_table.song_id, song_table.artist_id, \
                                                        log_table.sessionId as session_id, log_table.location, \
                                                        log_table.userAgent as user_agent \
                                                        from log_table \
                                                        join song_table on (log_table.artist = song_table.artist_name and \
                                                        log_table.song = song_table.title and log_table.length = song_table.duration )""")`

Dimensional tables for the schema included the following:
1. `users`: users in the app
2. `songs`: songs in music database
3. `artists`: artists in music database
4. `time`: timestamps of records in `songplays` broken down into specific units

## Contact
Created by @[peterndiforchu](https://www.linkedin.com/in/peter-ndiforchu-0b8986129) - feel free to contact me!
