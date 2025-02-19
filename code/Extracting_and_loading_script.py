import json
import os
import boto3
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def lambda_handler(event, context):
    
    # Spotify API credentials
    CLIENT_ID = os.environ.get('client_id')
    CLIENT_SECRET = os.environ.get('client_secret')

    # Authentication
    client_credentials_manager = SpotifyClientCredentials(client_id = CLIENT_ID, client_secret = CLIENT_SECRET)

    # Authorization
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

    playlist_url = "https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE"

    # We want the playlist id from the link as the function needs it to fetch the data
    playlist_uri = playlist_url.split("/")[-1]

    # For extracting the playlist data
    data = sp.playlist_tracks(playlist_uri)

    #setting file_name to be saved in s3 bucket
    file_name = "spotify_raw_" + str(datetime.now()) + ".json"

    client = boto3.client('s3')
    client.put_object( 
        Bucket = "spotify-project-spark",
        Key = "raw_data/to_process/" + file_name,
        Body = json.dumps(data)
    )


    # Triggering Glue job
    glue = boto3.client("glue")
    glueJobName = "spotify_transformation_job"

    try:
        runId = glue.start_job_run(JobName = glueJobName)
        status = glue.get_job_run(JobName = glueJobName, RunId = runId["JobRunId"])
        print("Job Status : ", status['JobRun']['JobRunState'])

    except Exception as e:
        print(e)