import tweepy
import logging
import os
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

    # your bearer token
    MY_BEARER_TOKEN = os.environ["TWITTER_BEARER_TOKEN"]
    # create your client 
    client = tweepy.Client(bearer_token=MY_BEARER_TOKEN)

    startDay = datetime.now() - timedelta(seconds=11)
    date_string = startDay.strftime('%Y-%m-%d')

    days = timedelta(days=1)

    # not actually the end day
    endDay = startDay - days

    # Define a dictionary containing students data
    data = {'show': ['Dune', 'TheWheelOfTime', 'SellingTampa', 'TheWitcher'],
                    'stream': ['HBO', 'AmazonPrime', 'Netflix', 'Netflix'],
                    'nonHash': ['Dune', 'Wheel of Time', 'Selling Tampa', 'Witcher']}

    # Convert the dictionary into DataFrame
    df = pd.DataFrame(data, columns = ['show','stream','nonHash'])

    tweets_df = pd.DataFrame()

    for i in range(len(df)) :

        # query to search for tweets
        query = ("#" + df.loc[i, "show"] + " OR " + df.loc[i, "nonHash"])+ "  -is:retweet"
        retweetQuery = ("#" + df.loc[i, "show"] + " OR " + df.loc[i, "nonHash"]) + "  is:retweet"

        # get tweets from the API
        tweets = client.get_recent_tweets_count(query=query,
                                            start_time=endDay,
                                            end_time=startDay,
                                            granularity = "day"
                                            )
        # get tweets from the API
        retweetdata = client.get_recent_tweets_count(query=retweetQuery,
                                            start_time=endDay,
                                            end_time=startDay,
                                            granularity = "day"
                                            )

        tweets_df_initial = pd.DataFrame(tweets.data)
        retweetDFrame = pd.DataFrame(retweetdata.data)

        tweets_df_initial['ShowName'] = df.loc[i, "show"]
        tweets_df_initial['Streamer'] = df.loc[i, "stream"]
        tweets_df_initial['Date'] = date_string
        tweets_df_initial['RetweetCount'] = retweetDFrame['tweet_count']

        tweets_df = tweets_df.append(tweets_df_initial, ignore_index=True)

    connect_str = os.environ["AZURE_STORAGE"]

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Create a unique name for the container
    container_name = "tweetcontainer"

    # Create a file in the local data directory to upload and download
    local_file_name =  "tweetCountTest" + ".txt"

    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=local_file_name)

    output = tweets_df.to_csv (index_label="idx", encoding = "utf-8")#,header = False)

    # Upload the created file
    blob_client.upload_blob(output,blob_type="AppendBlob")                  
