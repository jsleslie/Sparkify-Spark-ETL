import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, from_unixtime, row_number, lit
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.window import Window

# os.environ['jdk.xml.entityExpansionLimit']='0';  


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
    """
    This function reads song data files from the sparkify S3 bucket,
    transforms them into songs and artists tables and writes them
    as parquet files with appropriate partitions
    """
    
    # test with small dataset
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data files
    df =  spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    song_out = os.path.join(output_data, "songs")
    songs_table.write.parquet(song_out, partitionBy=("year","artist_id"))

    # extract columns to create artists table
    artists_table = df.select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']).dropDuplicates()
    
    # write artists table to parquet files
    artists_out = os.path.join(output_data, "artists")
    artists_table.write.parquet(artists_out)


def process_log_data(spark, input_data, output_data):
    """
    This function reads log data files from the sparkify S3 bucket,
    transforms them into the users, time and songplays tables and writes them
    as parquet files with appropriate partitions
    """
    # get filepath to log data file
    
    # test with small dataset
#     log_data = "s3n://sparkify/sparkify_log_small.json"
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # read log data file
    df =  spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(['userId', 'firstName', 'lastName', 'gender', 'level']).dropDuplicates()
    
    # output path
    users_out = os.path.join(output_data, "users")
    
    # write users table to parquet files
    users_table.write.parquet(users_out)

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn('start_time', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    df = df.withColumn('datetime', from_unixtime(df.start_time))
    
    df = df.withColumn("hour", hour(df.datetime))
    df = df.withColumn("day", dayofmonth(df.datetime))
    df = df.withColumn("week", weekofyear(df.datetime))
    df = df.withColumn("month", month(df.datetime))
    df = df.withColumn("year", year(df.datetime))
    df = df.withColumn("weekday", dayofweek(df.datetime))
    
    # extract columns to create time table
    time_table = df.select(['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']).dropDuplicates() 
    
    #output path
    time_out = os.path.join(output_data, "time")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(time_out, partitionBy=("year","month"))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/')


    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df,  df.song == song_df.title,how = 'left')
    
    w = Window().orderBy(lit('A'))
    songplays_table = songplays_table.withColumn("songplay_id", row_number().over(w))
    songplays_table = songplays_table.withColumn("user_id", df.userId)

    
    songplays_df = songplays_table.select(['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', col('sessionId').alias('session_id'),\
                                           'location', col('userAgent').alias('user_agent'), 'month', 'datetime'])
    songplays_df = songplays_df.withColumn("year", year(songplays_df.datetime)).drop("datetime")
      
    songplays_out = os.path.join(output_data, "songplays")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_df.write.parquet(songplays_out, partitionBy=("year", "month"))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-workspace/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
