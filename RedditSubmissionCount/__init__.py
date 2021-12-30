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

    submission_df = pd.DataFrame()

    for i in range(len(df)) :

        reddit = praw.Reddit(client_id= os.environ["REDDIT_CLIENT_ID"],
                    client_secret= os.environ["REDDIT_SECRET"],
                    user_agent='testscript for streaming tv to research engagement')
        
        subreddit = reddit.subreddit(df.loc[i, "subreddit"])
        show = df.loc[i, "show"]
        stream = df.loc[i, "stream"]
        # assume you have a Subreddit instance bound to variable `subreddit`
        for submission in subreddit.top(time_filter = 'day',limit=20):
                submission_initial = pd.DataFrame(columns=list(["Show","Stream","Date","NumComments","TotalScore","UpvoteRatio","Id","SubmissionDate"]))
                submission_initial.loc[0] = [show,stream,date_string,submission.num_comments,submission.score,submission.upvote_ratio,submission.id,str(datetime.utcfromtimestamp(submission.created_utc))]
                
                submission_df = submission_df.append(submission_initial, ignore_index=True)

    connect_str = os.environ["AZURE_STORAGE"]

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a unique name for the container
    container_name = "tweetcontainer"

    # Create a file in the local data directory to upload and download
    local_file_name =  "redditCommentCount" + ".txt"

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

    output = submission_df.to_csv (index_label="idx", encoding = "utf-8")#,header = False)

    # Upload the created file
    blob_client.upload_blob(output,blob_type="AppendBlob")   

    