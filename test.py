from pyspark.sql import SparkSession
from etl import output_data

def test_tables(spark, output_data):
    """ tests whether the output tables have correct columns"""  
    df = spark.read.parquet(output_data + 'songs')
    assert df.columns == ['song_id', 'title', 'duration', 'year', 'artist_id'], f"{df.schema.names} is incorrect"

    df = spark.read.parquet(output_data + 'artists')
    assert df.columns == ['song_id', 'title', 'artist_id', 'year', 'duration'], f"{df.schema.names} is incorrect"

    df = spark.read.parquet(output_data + 'users')
    assert df.columns == ['user_id', 'first_name', 'last_name', 'gender', 'level'], f"{df.schema.names} is incorrect"

    df = spark.read.parquet(output_data + 'times')
    assert df.columns == ['start_time', 'hour', 'day', 'weekday', 'week', 'year', 'month'], f"{df.schema.names} is incorrect"

    df = spark.read.parquet(output_data + 'songplays')
    assert df.columns == ['start_time', 'user_id', 'level', 'session_id', 'location', 
                               'user_agent', 'song_id', 'artist_id', 'year', 'month'], f"{df.schema.names} is incorrect"


if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .getOrCreate()
    test_tables(spark, output_data)
    spark.stop()
    