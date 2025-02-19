
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col, to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://spotify-project-spark/raw_data/to_process/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    connection_options = {"paths": [s3_path]},
    format = "json"
)
spotify_df = source_dyf.toDF()
# creating df so we don't directly perform operations on actual DF
df = spotify_df
## Below are the functions created for extracting the specific data from the DF
def process_album(df):
    df = df.withColumn("items", explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        to_date(col("items.track.album.release_date")).alias("release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")
    ).drop_duplicates(['album_id'])
    
    return df


def process_artist(df):
    #First, explode the items to get indivisual tracks
    df_items_exploded = df.select(explode(col("items")).alias("items"))
    
    #Then, explode the artists array within each item to create a row for each artist
    df_artist_exploded = df_items_exploded.select(explode(col("items.track.artists")).alias("artists"))
    
    #Now, select the artist attributes, ensuring each artist is in its own row
    df_artists = df_artist_exploded.select(
        col("artists.id").alias("artist_id"),
        col("artists.name").alias("artist_name"),
        col("artists.external_urls.spotify").alias("url")
    ).drop_duplicates(['artist_id'])
    
    return df_artists


def process_songs(df):
    #Explode the items array to create a row for each song
    df_song_exploded= df.select(explode(col("items")).alias("items"))
    
    #Extract song info from the exploded DataFrame
    df_songs = df_song_exploded.select(
        col("items.track.id").alias("song_id"),
        col("items.track.name").alias("song_name"),
        col("items.track.duration_ms").alias("duration_ms"),
        col("items.track.external_urls.spotify").alias("url"),
        col("items.track.popularity").alias("popularity"),
        col("items.added_at").alias("song_added"),
        col("items.track.album.id").alias("album_id"),
        col("items.track.artists")[0]["id"].alias("artist_id")
    ).drop_duplicates(['song_id'])
    
    #Convert string dates in "song_added" to actual date types
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    
    return df_songs
album_df = process_album(spotify_df)

artist_df = process_artist(spotify_df)

songs_df = process_songs(spotify_df)

def write_to_s3(df, path_suffix, format_type = "csv"):
    # converting DF into dynamic frame
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    
    #writing dynamic frame to s3
    glueContext.write_dynamic_frame.from_options(
        frame = dynamic_frame,
        connection_type = "s3",
        connection_options = {"path" : f"s3://spotify-project-spark/transformed_data/{path_suffix}/"},
        format = format_type
    )
#writing album to s3
write_to_s3(album_df, "album_data/album_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
#writing artist to s3
write_to_s3(artist_df, "artist_data/artist_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")
# writing songs to s3
write_to_s3(songs_df, "songs_data/songs_transformed_{}".format(datetime.now().strftime("%Y-%m-%d")), "csv")


def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket = bucket, Prefix = prefix)
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.json')]
    return keys

bucket = "spotify-project-spark"
prefix = "raw_data/to_process/"
spotify_keys = list_s3_objects(bucket, prefix)

def move_and_delete_files(spotify_keys, Bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : key
        }

        #Define the destination key
        destination_key = 'raw_data/processed/' + key.split("/")[-1]

        #Copy the file to the new location
        s3_resource.meta.client.copy(copy_source, Bucket, destination_key)

        #Delete the original file
        s3_resource.Object(Bucket, key).delete()

move_and_delete_files(spotify_keys, bucket)

job.commit()