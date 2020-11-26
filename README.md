# Tweet Archiver

Archive your tweets to AWS DynamoDB and make them searchable locally with Elasticsearch.

## Requirements

* AWS account
* Python 3
* Tweepy
* boto3

## Set Up

```bash
pip install tweepy boto3
```

### Environment Variables

You can get the Twitter consumer keys and access token from https://developer.twitter.com/en/apps.
Make sure you have the right privileges for retrieving tweets.

| Environment Variable        | Description               |
|-----------------------------|---------------------------|
| DAYS_TO_KEEP                | Number of days to keep tweet before unliked or deleted. If not set will not delete/unlike  |
| DYNAMODB_TABLE_NAME         | Name of DynamoDB table (default: Tweets)  |
| TWITTER_CONSUMER_KEY        | Key for authentication    |
| TWITTER_CONSUMER_SECRET_KEY | Secret for authentication |
| TWITTER_ACCESS_TOKEN        | Access privileges         |
| TWITTER_ACCESS_TOKEN_SECRET | Access privilege secret   |
| TWITTER_SCREEN_NAME         | Twitter handle            |

## Limitations

Twitter apparently only allows access to a user's most recent 3,240 tweets with this method.

## Reference

1. https://highlandsolutions.com/blog/hands-on-examples-for-working-with-dynamodb-boto3-and-python
2. https://dev.to/jeannienguyen/building-a-twitter-bot-with-python-and-aws-lambda-27jg

## License

MIT
