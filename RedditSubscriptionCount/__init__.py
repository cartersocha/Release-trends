import datetime
import logging
import os
import praw
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import azure.functions as func

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.utcnow().replace(
        tzinfo=timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    startDay = datetime.now() - timedelta(seconds=11)
    date_string = startDay.strftime('%Y-%m-%d')

    # Define a dictionary containing students data
    data = {'show': ['Dune', 'TheWheelOfTime', 'SellingTampa', 'TheWitcher'],
            'subreddit': ['Dune', 'WoT', 'SellingTampa', 'Witcher'],
                    'stream': ['HBO', 'AmazonPrime', 'Netflix', 'Netflix']}
    
    # Convert the dictionary into DataFrame
    df = pd.DataFrame(data, columns = ['show','subreddit','stream'])

    reddit = praw.Reddit(client_id= os.environ["REDDIT_CLIENT_ID"],
                    client_secret= os.environ["REDDIT_SECRET"],
                user_agent='testscript for streaming tv to research engagement')

    reddit_df = pd.DataFrame()

    for i in range(len(df)) :
    
        subreddit = reddit.subreddit(df.loc[i, "subreddit"])
        reddit_df_initial = pd.DataFrame(columns=list(["Show","Stream","Date","SubscriberCount"]))

        reddit_df_initial.loc[0] = [df.loc[i, "show"],df.loc[i, "stream"],date_string,subreddit.subscribers]

        reddit_df = reddit_df.append(reddit_df_initial, ignore_index=True)

    connect_str = os.environ["AZURE_STORAGE"]

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a unique name for the container
    container_name = "tweetcontainer"

    # Create a file in the local data directory to upload and download
    local_file_name =  "redditCountTest" + ".txt"

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

    output = reddit_df.to_csv (index_label="idx", encoding = "utf-8")#,header = False)

    # Upload the created file
    blob_client.upload_blob(output,blob_type="AppendBlob")     


    