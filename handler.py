import datetime
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
        ("DAYS_TO_KEEP", 365),
        ("DYNAMODB_TABLE_NAME", "Test6"),
        ("LOG_LEVEL", "INFO"),
        ("TWITTER_CONSUMER_KEY", ""),
        ("TWITTER_CONSUMER_SECRET_KEY", ""),
        ("TWITTER_ACCESS_TOKEN", "Defined "),
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

MAX_ID_KEY = 0


def create_dynamodb_table(table_name: str):
    """Create DynamoDB table"""
    dynamodb = boto3.resource("dynamodb")

    try:
        logger.info("creating table '%s'", table_name)
        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
            ProvisionedThroughput={
                "ReadCapacityUnits": 1,
                "WriteCapacityUnits": 1,
            },
        )
    except dynamodb.meta.client.exceptions.ResourceInUseException:
        logger.warning("table already exists: %s", table_name)
    else:
        logger.info("table status: %s", table.table_status)


def initialize_twitter_client():
    """Authorize twitter, initialize tweepy"""
    auth = tweepy.OAuthHandler(
        config["TWITTER_CONSUMER_KEY"], config["TWITTER_CONSUMER_SECRET_KEY"]
    )
    auth.set_access_token(
        config["TWITTER_ACCESS_TOKEN"], config["TWITTER_ACCESS_TOKEN_SECRET"]
    )
    api = tweepy.API(auth, wait_on_rate_limit_notify=True, wait_on_rate_limit=True)

    try:
        api.me()
    except tweepy.error.TweepError as e:
        logger.error("please check the authentication information:\n%s", str(e))
    else:
        return api


def get_tweets(name: str, query_id: int = None) -> List[dict]:
    """Get tweets of a given screen name including retweets"""

    # If no query id is specified that means we need to retrive all tweets and
    # should use max_id
    if query_id is None:
        query_key = "max_id"
    else:
        query_key = "since_id"

    api = initialize_twitter_client()

    tweets = []

    # Twitter only allows access to a users most recent 3240 tweets with this method
    # keep grabbing tweets until there are no tweets left to grab; max count is 200
    while len(arr := api.user_timeline(name, count=200, **{query_key: query_id})) > 0:
        tweets.extend(arr)

        if query_key == "max_id":
            query_id = arr[-1].id - 1
        else:
            query_id = arr[0].id

        logger.info("%d tweets downloaded so far", len(tweets))

        # TODO remove
        break

    return tweets


def get_max_id_stored(table_name: str):
    """Get the highest tweet id stored in DynamoDB"""
    dynamodb = boto3.resource("dynamodb")

    table = dynamodb.Table(table_name)

    # basic retry logic
    attempts = 4
    for i in range(1, attempts + 1):
        try:
            # get latest id in descending order
            response = table.get_item(Key={"id": MAX_ID_KEY})
        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            sleep = i * 5
            attempts -= 1
            logger.info("table not found. retrying in %d seconds. attempt #: %d", sleep, attempts)
            time.sleep(sleep)
            continue
        else:
            break

    logger.debug(response)
    if "Item" not in response:
        return None
    else:
        return response["Item"]["max_id"]


def put_tweets_in_dynamodb(table_name: str, tweets: List[dict]) -> None:
    """Write tweets in DynamoDB"""
    # create dynamodb resource object
    client = boto3.resource("dynamodb")
    table = client.Table(table_name)

    assert len(tweets) > 0, "tweets must not be empty"

    max_id = tweets[0].id

    with table.batch_writer() as batch:
        for tweet in tweets:
            max_id = max(max_id, tweet.id)
            table.put_item(
                Item={"id": tweet.id, "json": json.dumps(tweet._json)}
            )

    return max_id


def store_max_id(table_name: str, max_id: int):
    # create dynamodb resource object
    client = boto3.resource("dynamodb")
    table = client.Table(table_name)

    # store Max ID
    table.put_item(Item={"id": MAX_ID_KEY, "max_id": max_id})


def delete_tweets(api: tweepy.API, days_to_keep: int):
    """Delete tweets in DynamoDB older than a certain date"""
    # Scan for tweets not deleted beofore a certain date
    # Delete tweets
    # Add attribute as deleted in dynamodb
    # TODO
    cutoff_date = datetime.datetime.utcnow() - datetime.timedelta(days=days_to_keep)

    return


if __name__ == "__main__":
    create_dynamodb_table(config["DYNAMODB_TABLE_NAME"])

    max_id = get_max_id_stored(config["DYNAMODB_TABLE_NAME"])

    logger.info("found max id: %s", max_id)
    tweets = get_tweets(config["TWITTER_SCREEN_NAME"], max_id)

    logger.info("%d tweets found", len(tweets))

    if tweets:
        new_max_id = put_tweets_in_dynamodb(config["DYNAMODB_TABLE_NAME"], tweets)

        logger.info("updating max_id to %d", new_max_id)
        store_max_id(config["DYNAMODB_TABLE_NAME"], new_max_id)

    # TODO
    # delete_tweets(int(config["ARCHIVE_DAYS"]))
