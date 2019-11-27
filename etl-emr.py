#Load modules
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F
import os
import configparser
import zipfile
import datetime
from pyspark.sql.functions import hour, year, month, dayofmonth, weekofyear, dayofweek
from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

# Set AWS key and secret key
os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    '''
    This function creates our spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_data(spark, input_data, output_data):
    '''
    Input:
    spark = Sparksession
    input_data = file path to S3 where we read the data
    output_data = Filepath to S3 where we write the data
    This function will read the song and log data from S3, then creates
    5 analytics tables and writes them to S3.
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    print('Reading Song Data ... \n')
    
    # S3 data
    df_song = spark.read.json(song_data)
    
    # Zip data for testing
    #df_song = spark.read.json('unzip_data/song_data/song_data/A/*/*/*.json')
    
    #################################################
    # get filepath to log data file
    #S3 data
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    print('Loading Log data ... \n')
    
    # S3 data
    df_log = spark.read.json(log_data)
    
    # Zip data for testing
    #df_log = spark.read.json('unzip_data/log_data/*.json')
    # get start time
    df_log = df_log.withColumn('start_time', F.from_unixtime(df_log['ts']/1000).cast("timestamp")).filter(df_log['page']=='NextSong')    
    ###################################################
    print('Creating Song Table ... \n')
    # extract columns to create songs table   
    songs_table = df_song.select(['song_id','title','artist_id','year','duration'])\
    .dropDuplicates(subset=['song_id']).sort('title')
    
    #################################################
    print('Creating Artist Table ... \n')
    #extract columns to create artists table
    artist_table = df_song.select(['artist_id','artist_name',\
                                     'artist_location','artist_latitude','artist_longitude'])\
    .dropDuplicates(subset=['artist_id']).sort('artist_name')
    artist_table = artist_table.selectExpr('artist_id as artist_id','artist_name as name',\
                                                'artist_location as location','artist_latitude as latitude',\
                                                'artist_longitude as longitude')  

    #################################################
    print('Creating User Table ... \n')
    # extract columns for users table  
    user_table = df_log.select(['userId','firstName','lastName',\
                                  'gender','level'])\
    .dropDuplicates(subset=['userId']).sort('lastName').where(df_log['userId']!= '')

    # Change userid type from string to integer
    user_table = user_table.withColumn("userId", user_table["userId"].cast(IntegerType()))

    ####################################################
    #Create time table
    # Get start time
    print('Creating Time Table ... \n')
    time_table = df_log.select(['start_time'])

    # Convert from start time to hour/day/week/month/year/weekday
    time_table = time_table.select(
        'start_time',
        hour("start_time").alias('hour'),
        dayofmonth("start_time").alias('day'),
        weekofyear("start_time").alias('week'),
        month("start_time").alias('month'),
        year("start_time").alias('year'), 
        dayofweek("start_time").alias('weekday')
    )    
    
    ######################################################
    print('Creating Songplays Table ... \n')
    # Get data from log data
    songplays_table = df_log.select(['start_time','userId','level','song','sessionId','location','userAgent','length']).filter(df_log['page']=='NextSong')

    #get song, songid, and artistid to join
    song_artist_table = df_song.select(['title','song_id','artist_id','duration']).dropDuplicates(subset=['song_id'])

    # Create songplay_id column
    songplays_table = songplays_table.withColumn("songplay_id", F.monotonically_increasing_id())

    #join songid and artistid
    songplays_table = songplays_table.join(song_artist_table,\
                                           songplays_table.song == song_artist_table.title,\
                                           how='left')

    #drop ts song and title columns
    songplays_table = songplays_table.drop('title')

    # Change userid type from string to integer
    songplays_table = songplays_table.withColumn("userId", songplays_table["userId"].cast(IntegerType()))

    # create month column
    songplays_table = songplays_table.withColumn("month",\
                                                 month(songplays_table["start_time"]).alias('month'))   
    # create year column
    songplays_table = songplays_table.withColumn("year",\
                                                 year(songplays_table["start_time"]).alias('year'))
    ###################################################
    # Write data

    print('Writing Artist Table ... \n')
    artist_table.write.mode("overwrite").parquet(output_data+'artist_table')
    print('Writing User Table ... \n')
    user_table.write.mode("overwrite").parquet(output_data+'user_table')
    print('Writing Songplays Table ... \n')
    songplays_table.write.mode("overwrite").partitionBy('year','month').parquet(output_data+'songplays_table') 
    print('Writing Time Table ... \n')
    time_table.write.mode("overwrite").partitionBy('year','month').parquet(output_data+'time_table')
    print('Writing Song Table ... \n')
    songs_table.write.mode("overwrite").partitionBy('year','artist_id').parquet(output_data+'songs_table')

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-datalakes-project/"
    
    process_data(spark, input_data, output_data) 
    print('etl.py Completed! \n')


if __name__ == "__main__":
    main()
