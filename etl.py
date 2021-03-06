import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DateType,
    TimestampType,
)


@F.udf
def get_timestamp(dt):
    return datetime.fromtimestamp(dt / 1000.0).isoformat()


@F.udf
def get_weekday(datetime_with_timezone):
    return datetime.strptime(datetime_with_timezone, '%Y-%m-%dT%H:%M:%S.%f').strftime('%w')


def get_songs_table_path(output_data):
    return os.path.join(output_data, 'songs.parquet')


def get_artists_table_path(output_data):
    return os.path.join(output_data, 'artists.parquet')


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

SONG_SCHEMA = StructType([
    # Artist ID
    StructField('artist_id', StringType()),

    # Song-related
    StructField('title', StringType()),
    StructField('year', IntegerType()),
    StructField('duration', DoubleType()),

    StructField('num_songs', IntegerType()),

    # Artist-related
    StructField('artist_name', StringType()),
    StructField('artist_location', StringType()),
    StructField('artist_latitude', DoubleType()),
    StructField('artist_longitude', DoubleType()),
])


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data, song_schema):
    """
        Loads song data from S3 and then extracts the relevant fields to create
        two tables: songs and artists, stored as parquet files (also on S3)

        The *songs table* has the following columns:
            (artist_id, title, year, duration)

        whereas the *artists table* has the following:
            (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
        """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')

    # read song data file
    print('Getting the song data from "{}"...'.format(song_data))
    df = spark.read.json(song_data, schema=song_schema)
    print('Done getting song data!')

    # extract columns to create songs table
    songs_df = (
        df
        .select(['artist_id', 'title', 'year', 'duration'])
        .dropDuplicates()
        .withColumn('song_id', F.monotonically_increasing_id())
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table_path = get_songs_table_path(output_data)
    print('Dumping the songs table')
    songs_df \
        .write \
        .partitionBy('year', 'artist_id') \
        .parquet(songs_table_path, mode='overwrite')
    print('Done!')

    # extract columns to create artists table
    artists_df = (
        df
        .select(['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude'])
        .dropDuplicates()
    )

    # write artists table to parquet files
    artists_table_path = get_artists_table_path(output_data)
    print('Dumping the artists table')
    artists_df \
        .write \
        .parquet(artists_table_path, mode='overwrite')
    print('Done!')


def process_log_data(spark, input_data, output_data):
    """
        TODO: fill out
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    print('Getting the log data from "{}"'.format(log_data))
    df = spark.read.json(log_data)
    print('Done!')

    # filter by actions for song plays
    df = df.filter(F.col('page') == 'NextSong')
#     import ipdb; ipdb.set_trace()

    # extract columns for users table
    users_df = (
        df
        .select(['userId', 'firstName', 'lastName', 'gender', 'level'])
        .dropDuplicates()
    )

    # write users table to parquet files
    users_table_path = os.path.join(output_data, 'users.parquet')
    print('Dumping users table')
    users_df.write.parquet(users_table_path, mode='overwrite')
    print('Done!')

    # create timestamp column from original timestamp column
    print('Extracting start_time from ts')
    time_df = (
        df
        .select('ts')
        .withColumn('start_time', get_timestamp('ts'))
    )
    print('Done')

    # extract columns to create time table
    print('Extracting time columns from start_time')
    time_df = (
        time_df
        .withColumn('hour', F.hour('start_time'))
        .withColumn('day', F.dayofmonth('start_time'))
        .withColumn('week', F.weekofyear('start_time'))
        .withColumn('month', F.month('start_time'))
        .withColumn('year', F.year('start_time'))
        .withColumn('weekday', get_weekday('start_time'))
    )
    print('Done')

    # write time table to parquet files partitioned by year and month
    print('Writing time_df to S3')
    time_table_path = os.path.join(output_data, 'time.parquet')
    time_df \
        .write \
        .partitionBy('year', 'month') \
        .parquet(time_table_path, mode='overwrite')
    print('Done')

    # read in song data to use for songplays table
    songs_table_path = get_songs_table_path(output_data)
    print('Reading the songs table fron S3')
    songs_df = spark.read.parquet(songs_table_path)
    print('Done')

    # get the log data frame
    print('Getting the log data frame')
    log_df = (
        df
        .withColumn('songplay_id', monotonically_increasing_id())
        .withColumn('start_time', get_timestamp('ts'))
        .withColumn('month', month('start_time'))
        .select([
            'userId', 'sessionId', 'userAgent', 'length', 'artist', 'location',
            'song', 'level', 'songplay_id', 'start_time', 'month',
        ])
    )
    print('Done')

    # extract columns from joined song and log datasets to create songplays table
    print('Joining the log_df and songs_df to get the songplays_df')
    songplays_df = (
        log_df
        .join(
            songs_df,
            (log_df.song == songs_df.title) & (log_df.length == songs_df.duration)
        )
        .select([
            'songplay_id', 'start_time', 'user_id', 'level', 'song_id',
            'artist_id', 'session_id', 'location', 'user_agent',
            'year', 'month'
        ])
    )
    print('Done')

    print('Num rows in songplays_df: {}'.format(songplays_df.count()))

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path = os.path.join(output_data, 'songplays.parquet')
    print('Wrinting the songplays data frame tp S3')
    songplays_df \
        .write \
        .partitionBy('year', 'month') \
        .parquet(songplays_table_path, mode='overwrite')
    print('Done')


def main():
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://for-data-engineering-nanodegree/dend_project_4'

    process_song_data(spark, input_data, output_data, SONG_SCHEMA)

    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
