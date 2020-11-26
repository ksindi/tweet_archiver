import boto3
from elasticsearch import Elasticsearch

if __name__ == "__main__":

    config = {
        k: os.getenv(k, default)
        for k, default in [
            ("APP_ENV", "local"),
            ("DYNAMODB_TABLE_NAME", "Tweets"),
            ("LOG_LEVEL", "INFO"),
        ]
    }

    INDEX_NAME = "tweets"

    # by default we connect to localhost:9200
    es = Elasticsearch()
    # create an index in elasticsearch, ignore status code 400 (index already exists)
    es.indices.create(index=INDEX_NAME, ignore=400)

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(config["DYNAMODB_TABLE_NAME"])

    response = table.scan()
    data = response["Items"]

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        tweets = response["Items"]

        for tweet in tweets:
            # have ES return a response when it indexes this document
            response = elastic.index(
                index=INDEX_NAME, doc_type="person", id=tweet["id"], body=tweet
            )
