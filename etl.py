import configparser
import os
from termcolor import colored
from queries import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import from_unixtime, hour, dayofmonth, dayofweek, weekofyear, month, year


config = configparser.ConfigParser()
config.read('config.ini')

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]

input_data_song = "s3a://udacity-dend/song_data/A/B/A/*.json"
input_data_log = "s3a://udacity-dend/log_data/2018/11/*.json"
output_data = "output_data/"

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
    print(colored("--------------- start loading song data ---------------", 'green'))
    df = spark.read.json(input_data_song)
    
    # create a temp view
    df.createOrReplaceTempView('tmp')
    
    # extract columns to create songs table
    songs_table = spark.sql(songs_table_query)
    songs_table = songs_table.dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    print(colored("--------------- writing songs table ---------------", 'blue'))
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'))

    # extract columns to create artists table
    artists_table = spark.sql(artists_table_query)
    
    # write artists table to parquet files
    print(colored("--------------- writing artists table ---------------", 'blue'))
    artists_table.write.parquet(os.path.join(output_data, 'artists'))
    artists_table = artists_table.dropDuplicates(['artist_id'])


def process_log_data(spark, input_data_log, output_data):
    """ 
    1. takes a spark session, input data path, output data path
    2. reads log data from input_data and songs table that is created in process_song_data
    3. processes the data, creates users, time, and songplays tables
    4. loads the tables to output_data
    """
    
    # read log data file
    print(colored("--------------- start loading log data ---------------", 'green'))
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
    print(colored("--------------- writing users table ---------------", 'blue'))
    users_table.write.parquet(os.path.join(output_data, 'users'))

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
    print(colored("--------------- writing time table ---------------", 'blue'))
    times_table.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'times'))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs')) 

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView('tmp2')
    songplays_table = spark.sql(songplays_table_query)

    # write songplays table to parquet files partitioned by year and month
    print(colored("--------------- writing songplays table ---------------", 'blue'))
    songplays_table.withColumn('year', year(df.start_time))\
    .withColumn('month', month(df.start_time))\
    .write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'songplays'))

def main():
    spark = create_spark_session()
    process_song_data(spark, input_data_song, output_data)    
    process_log_data(spark, input_data_log, output_data)


if __name__ == "__main__":
    main()
