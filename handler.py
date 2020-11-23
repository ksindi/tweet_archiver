import json
import logging
import os
import sys
import time
from typing import List

import boto3
from boto3.dynamodb.conditions import Key, Attr
import tweepy

config = {
    k: os.getenv(k, default)
    for k, default in [
        ("APP_ENV", "local"),
        ("ARCHIVE_DAYS", 365),
        ("DYNAMODB_TABLE_NAME", "Test1"),
        ("LOG_LEVEL", "INFO"),
        ("TWITTER_CONSUMER_KEY", ""),
        ("TWITTER_CONSUMER_SECRET_KEY", ""),
        ("TWITTER_ACCESS_TOKEN", ""),
        ("TWITTER_ACCESS_TOKEN_SECRET", ""),
        ("TWITTER_SCREEN_NAME", "kamilsindi"),
    ]
}

# set up logging
logging.Formatter.converter = time.gmtime
logging.basicConfig(
    format="%(asctime)s %(levelname)8s: %(message)s",
    stream=sys.stdout,
    level=config["LOG_LEVEL"],
)
logger = logging.getLogger(__name__)


def create_dynamodb_table(table_name: str):
    """Create DynamoDB table"""
    dynamodb = boto3.resource("dynamodb")

    try:
        logger.info("creating table '%s'", table_name)
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "id_str", "KeyType": "HASH"}, {"AttributeName": "id", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "id_str", "AttributeType": "S"}, {"AttributeName": "id", "AttributeType": "N"}],
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        )
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        logger.warning("table already exists: %s", table_name)
    else:
        logger.info("table status: %s", table.table_status)


def initialize_twitter_client(config: dict):
    """Authorize twitter, initialize tweepy"""
    auth = tweepy.OAuthHandler(
        config["TWITTER_CONSUMER_KEY"], config["TWITTER_CONSUMER_SECRET_KEY"]
    )
    auth.set_access_token(
        config["TWITTER_ACCESS_TOKEN"], config["TWITTER_ACCESS_TOKEN_SECRET"]
    )
    return tweepy.API(auth)


def get_tweets(api: tweepy.API, name: str, since_id: int = 0) -> List[dict]:
    """Get tweets of a given screen name including retweets"""
    tweets = []

    # TODO
    # Twitter only allows access to a users most recent 3240 tweets with this method
    # Keep grabbing tweets until there are no tweets left to grab; max count is 200
    while (len(arr := api.user_timeline(screen_name=name, count=200, since_id=since_id)) > 0):
        tweets.extend(arr)
        since_id = arr[0].id
        logger.info("...%d tweets downloaded so far", len(tweets))

    return tweets


def get_max_id_stored(api: tweepy.API, table_name: str):
    """Get the highest tweet id stored in DynamoDB"""
    dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table(table_name)
    # get latest id in descending order
    response = table.query(
        KeyConditionExpression="id = :id", ScanIndexForward=False, Limit=1
    )
    return response["Items"][0]["id"]


def put_tweets_in_dynamodb(table_name: str, tweets: List[dict]) -> None:
    """Write tweets in DynamoDB"""
    # This will create dynamodb resource object
    client = boto3.resource("dynamodb")
    table = client.Table(table_name)

    for tweet in tweets:
        table.put_item(Item={"id": tweet.id, "tweet": tweet._json})


def delete_tweets(api: tweepy.API, archive_days: int):
    """Delete tweets in DynamoDB older than a certain date"""
    # Scan for tweets not deleted beofore a certain date
    # Delete tweets
    # Add attribute as deleted in dynamodb
    return


if __name__ == "__main__":
    create_dynamodb_table(config["DYNAMODB_TABLE_NAME"])

    api = initialize_twitter_client(config)

    max_id = get_max_id_stored(api, config["DYNAMODB_TABLE_NAME"])

    logger.info(max_id)

    exit(0)

    tweets = get_tweets(api, config["TWITTER_SCREEN_NAME"], max_id)

    put_tweets_in_dynamodb(config["DYNAMODB_TABLE_NAME"], tweets)
    # delete_tweets(int(config["ARCHIVE_DAYS"]))
