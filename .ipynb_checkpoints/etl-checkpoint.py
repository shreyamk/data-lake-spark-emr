import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''Function to create Spark session'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''Function to read/write data for songs, artists tables from/on to S3 buckets in the given file paths'''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    
    # creating view for writing SQL statements
    df.createOrReplaceTempView("song_data_table")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT sd.song_id, 
                            sd.title,
                            sd.artist_id,
                            sd.year,
                            sd.duration
                            FROM song_data_table sd
                            WHERE sd.song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT ar.artist_id, 
                                ar.artist_name,
                                ar.artist_location,
                                ar.artist_latitude,
                                ar.artist_longitude
                                FROM song_data_table ar
                                WHERE ar.artist_id IS NOT NULL
                            """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    '''Function to read/write data for song play, time, users tables from/on to S3 buckets in the given file paths'''
    # get filepath to log data file
    log_path = input_data + 'log_data/*.json'

    # read log data file
    df = spark.read.json(log_path)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # Create view for SQL statements
    df.createOrReplaceTempView('log_data')
    
    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT u.userId as user_id, 
                            u.firstName as first_name,
                            u.lastName as last_name,
                            u.gender as gender,
                            u.level as level
                            FROM log_data u
                            WHERE u.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

#     # create timestamp column from original timestamp column
#     get_timestamp = udf()
#     df = 
    
#     # create datetime column from original timestamp column
#     get_datetime = udf()
#     df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            A.start_time_sub as start_time,
                            hour(A.start_time_sub) as hour,
                            dayofmonth(A.start_time_sub) as day,
                            weekofyear(A.start_time_sub) as week,
                            month(A.start_time_sub) as month,
                            year(A.start_time_sub) as year,
                            dayofweek(A.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(timeSt.ts/1000) as start_time_sub
                            FROM log_data_table timeSt
                            WHERE timeSt.ts IS NOT NULL
                            ) A
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data+'songs_table/')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(lg.ts/1000) as start_time,
                                month(to_timestamp(lg.ts/1000)) as month,
                                year(to_timestamp(lg.ts/1000)) as year,
                                lg.userId as user_id,
                                lg.level as level,
                                so.song_id as song_id,
                                so.artist_id as artist_id,
                                lg.sessionId as session_id,
                                lg.location as location,
                                lg.userAgent as user_agent
                                FROM log_data_table lg
                                JOIN song_data_table so on lg.artist = so.artist_name and lg.song = so.title
                            """) 

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    '''Main function to call other functions'''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://udendoutputdl/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
