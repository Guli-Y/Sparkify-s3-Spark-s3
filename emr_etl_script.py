#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import from_unixtime, hour, dayofmonth, dayofweek, weekofyear, month, year

input_data_song = "s3a://udacity-dend/song_data/*/*/*/*.json"
input_data_log = "s3a://udacity-dend/log_data/*/*/*.json"
output_data = "s3a://test-sparkify/"


songs_table_query = """SELECT
                           song_id,
                           title,
                           artist_id,
                           year,
                           duration
                       FROM tmp
                       WHERE song_id IS NOT NULL
                    """
artists_table_query = """SELECT
                           song_id,
                           title,
                           artist_id,
                           year,
                           duration
                       FROM tmp
                       WHERE song_id IS NOT NULL
                    """
users_table_query = """SELECT
                           userId as user_id,
                           firstName as first_name,
                           lastName as last_name,
                           gender,
                           level
                       FROM tmp
                       WHERE userId IS NOT NULL
                   """
times_table_query = """SELECT
                       start_time,
                       hour,
                       day,
                       weekday,
                       week,
                       month,
                       year
                    FROM tmp 
                """
songplays_table_query = """SELECT
                               t.start_time,
                               t.user_id,
                               t.level,
                               t.sessionId as session_id,
                               t.location,
                               t.userAgent as user_agent,
                               t2.song_id,
                               t2.artist_id
                           FROM tmp t
                             INNER JOIN tmp2 t2 
                                   ON t.song=t2.title 
                                   AND t.length=t2.duration
                           WHERE t.user_id IS NOT NULL
                               AND t2.artist_id IS NOT NULL"""

def create_spark_session():
    """ creates a spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data_song, output_data):
    """ 
    1. takes a spark session and reads song data from input_data
    2. processes the data and creates songs and artists tables
    3. loads songs and artists tables to output_data
    """
    
    # read song data file
    df = spark.read.json(input_data_song)
    
    # create a temp view
    df.createOrReplaceTempView('tmp')
    
    # extract columns to create songs table
    songs_table = spark.sql(songs_table_query)
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data +'songs')

    # extract columns to create artists table
    artists_table = spark.sql(artists_table_query)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data +'artists')
    artists_table = artists_table.dropDuplicates(['artist_id'])


def process_log_data(spark, input_data_log, output_data):
    """ 
    1. takes a spark session, input data path, output data path
    2. reads log data from input_data and songs table that is created in process_song_data
    3. processes the data, creates users, time, and songplays tables
    4. loads the tables to output_data
    """
    
    # read log data file
    df = spark.read.json(input_data_log)    
    
    # cast user id to int
    df = df.withColumn('user_id', df['userId'].cast('int'))

    # filter by actions for song plays
    df = df.where(df.page=="NextSong")

    # create tmp view
    df.createOrReplaceTempView('tmp')

    # extract columns for users table    
    users_table = spark.sql(users_table_query)
    users_table = users_table.dropDuplicates(['user_id'])

    # write users table to parquet files
    users_table.write.parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    df = df.withColumn('start_time', from_unixtime(df.ts/1000.0))
    df = df.withColumn('hour', hour(df.start_time))
    df = df.withColumn('day', dayofmonth(df.start_time))
    df = df.withColumn('week', weekofyear(df.start_time))
    df = df.withColumn('weekday', dayofweek(df.start_time))
    df = df.withColumn('month', month(df.start_time))
    df = df.withColumn('year', year(df.start_time))

    # create a temp view
    df.createOrReplaceTempView('tmp')

    # extract columns to create time table
    times_table = spark.sql(times_table_query)
    times_table = times_table.dropDuplicates(['start_time'])

    # write time table to parquet files partitioned by year and month
    times_table.write.partitionBy(['year', 'month']).parquet(output_data + 'times')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data +'songs')

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('tmp2')
    songplays_table = spark.sql(songplays_table_query)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.withColumn('year', year(df.start_time))\
    .withColumn('month', month(df.start_time))\
    .write.partitionBy(['year', 'month']).parquet(output_data + 'songplays')


def test_tables(spark, output_data):
    """ tests whether the output tables have correct columns"""  
    
    df = spark.read.parquet(output_data + 'songs')
    if df.columns == ['song_id', 'title', 'duration', 'year', 'artist_id']:
        print("SONGS TABLES -------------------------------------  LOADED CORRECTLY")

    df = spark.read.parquet(output_data + 'artists')
    if df.columns == ['song_id', 'title', 'artist_id', 'year', 'duration']:
        print("ARTISTS TABLES -----------------------------------  LOADED CORRECTLY")

    df = spark.read.parquet(output_data + 'users')
    if df.columns == ['user_id', 'first_name', 'last_name', 'gender', 'level']:
        print("USERS TABLES -------------------------------------  LOADED CORRECTLY")

    df = spark.read.parquet(output_data + 'times')
    if df.columns == ['start_time', 'hour', 'day', 'weekday', 'week', 'year', 'month']:
        print("TIME TABLES --------------------------------------  LOADED CORRECTLY")

    df = spark.read.parquet(output_data + 'songplays')
    if df.columns == ['start_time', 'user_id', 'level', 'session_id', 'location', 
                               'user_agent', 'song_id', 'artist_id', 'year', 'month']:
        print("SONGLAYS TABLES ----------------------------------  LOADED CORRECTLY")

if __name__ == "__main__":
    """extracts the data from s3 input_data path, 
       processes them, 
       creates new tables,
       stores them back to s3 output_data path as parquet
       and reads the output tables to check whether have the correct columns
    """
    spark = create_spark_session()
    process_song_data(spark, input_data_song, output_data)    
    process_log_data(spark, input_data_log, output_data)
    test_tables(spark, output_data)
    spark.stop()