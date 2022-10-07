import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']

os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
        This function creates a Spark session to give us access to S3.
        Returns a Spark object.
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    ''''
    This function 
    gets filepath to song data file then read it.
    Extracts columns to create songs table then write songs table to parquet files partitioned by year and artist.
    extract columns to create artists table then write artists table to parquet files. 
    Arguments : spark, input_data, output_data
     
    '''
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table =  df.select('song_id','title','artist_id','year','duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    
    ''''
    This function 
    gets filepath to log data file then read it.
    filter by actions for song plays and extract columns for users table. Then write users table to parquet files.  
    create timestamp column from original timestamp column and extract columns to create time table. Then write time table to parquet files partitioned by year and month.
    read in song data to use for songplays table, extract columns from joined song and log datasets to create songplays table, and then
    write songplays table to parquet files partitioned by year and month.
    Arguments : spark, input_data, output_data
     
    '''
    
    
    # get filepath to log data file
    log_data =  input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp((x / 1000.0)), T.TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))
   
    # create datetime column from original timestamp column
    df = df.withColumn("date", to_date(col("start_time")))
    
    # extract columns to create time table
    time_table =  df.select(
        "start_time",
        hour("start_time").alias("hour"),
        dayofmonth("date").alias("day"),
        weekofyear("date").alias("week"),
        month("date").alias("month"),
        year("date").alias("year"),
        dayofweek("date").alias("weekday"),
    )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')


    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')
    
    
    # I used some help to write time_table and songplays_table here are the links:
    # https://knowledge.udacity.com/questions/801263
    # https://knowledge.udacity.com/questions/610766
    # https://knowledge.udacity.com/questions/175498
    # https://stackoverflow.com/questions/43731679/how-to-save-a-partitioned-parquet-file-in-spark-2-1

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (song_df.title == df.song)).select(
        df.start_time,
        col("userId").alias("user_id"),
        song_df.song_id,
        song_df.artist_id,
        df.level,
        col("sessionId").alias("session_id"),
        df.location,
        col("userAgent").alias("user_agent"),
        year("date").alias("year"),
        month("date").alias("month"),
    )

    # write songplays table to parquet files partitioned by year and month
    #songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')
    songplays_table.createOrReplaceTempView('songs')
    songplays_table.parquet(output_data + 'songplay/songplay.parquet', 'overwrite')

    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://shireen-bucket/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
