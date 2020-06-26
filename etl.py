import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format
from pyspark.sql.functions import monotonically_increasing_id

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, song_input_data, output_data):
    """
    Reads JSON files located at song data directory.
    Extracts relevent columns for "songs", removes duplicates.
    Writes song data, partitioned by year and artist ID.
    Extracts relevent columns for "artists", removes duplicates.
    Writes artist data.  
    """
    # get filepath to song data file
    song_data = song_input_data
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration']
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table = df['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')

    
def process_log_data(spark, song_input_data, log_input_data, output_data):
    """
    Reads JSON files located at log data directory.
    Filters log data to only contain song plays (page = NextSong)
    Extracts relevent columns for "users", removes duplicates.    
    Writes user data.     
    Computes timestamp and datetime data from original timestamp, and adds to dataframe.
    Extracts datetime column, and decomposes into date heirarchy to create new "time" table.
    Writes time data, partitioned by year and month.    
    Reads JSON files located at song data directory.
    Joins log data frame to songs data frame, on song title and artist name match.
    Forms somngplays table, creating aliases for column names, and adding unique primary key to each record.
    Writes songplay data, partitioned by year and month. 
    """
    # get filepath to log data file
    log_data = log_input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.dropDuplicates(['userId', 'level'])
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000)))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select('datetime') \
                           .withColumn('start_time', df.datetime) \
                           .withColumn('hour', hour('datetime')) \
                           .withColumn('day', dayofmonth('datetime')) \
                           .withColumn('week', weekofyear('datetime')) \
                           .withColumn('month', month('datetime')) \
                           .withColumn('year', year('datetime')) \
                           .withColumn('weekday', dayofweek('datetime'))
    time_table = time_table.dropDuplicates(['datetime'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(song_input_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_df = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title))
    songplays_df = songplays_df.withColumn('month', month('datetime'))
    songplays_df = songplays_df.withColumn('songplay_id', monotonically_increasing_id())

    songplays_table = songplays_df.select(
        col('songplay_id').alias('songplay_id'),
        col('datetime').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'), 
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        col('month').alias('month'))


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')
    print("Songplays table written.")


def main():
    """
    Creates spark session/
    Converts song JSON data to artist and song tables.
    Converts log data to users and time tables.
    Converts song data and log data to songplays table.    
    """
    spark = create_spark_session()
    #song_input_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    #log_input_data = "s3a://udacity-dend/log_data/*.json"    
    song_input_data = "data/song_data/*.json"
    log_input_data = "data/log_data/*.json"       
    output_data = "s3a://aws-logs-895935180171-us-west-2/"
    process_song_data(spark, song_input_data, output_data)    
    process_log_data(spark, song_input_data, log_input_data, output_data)


if __name__ == "__main__":
    main()
