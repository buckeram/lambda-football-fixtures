import os
import boto3
import botocore
import logging
import requests
from datetime import datetime, timezone


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

s3 = boto3.resource('s3')


def lambda_handler(event, context):

    # Retrieve and log environment variables
    DATA_URL = os.environ['DATA_URL']
    LOGGER.info('DATA_URL: %s', DATA_URL)
    S3_BUCKET = os.environ['S3_BUCKET']
    LOGGER.info('S3_BUCKET: %s', S3_BUCKET)
    S3_KEY = os.environ['S3_KEY']
    LOGGER.info('S3_KEY: %s', S3_KEY)

    # Check last-modified time of our copy of the file
    our_last_modified = _get_last_modified(S3_BUCKET, S3_KEY)
    LOGGER.info("%s/%s last modified: %s" , S3_BUCKET, S3_KEY, our_last_modified)

    # Download fixtures
    latest_fixtures = requests.get(DATA_URL)
    latest_fixtures.raise_for_status()
    if latest_fixtures.status_code != 200:
        raise Exception("Can't download from %s: status_code=%d, reason=%s" %
                        (DATA_URL, latest_fixtures.status_code, latest_fixtures.reason))

    # Compare last-modified dates; if latest_fixtures is newer, then write it to S3
    # (Not sure if I'm doing the right thing here with UTC -- prob. should be BST)
    their_last_modified = datetime.strptime(
        latest_fixtures.headers['Last-Modified'], "%a, %d %b %Y %H:%M:%S %Z").astimezone(timezone.utc)
    LOGGER.info("%s last modified: %s", DATA_URL, their_last_modified)

    result = "No change to fixtures"
    if (not our_last_modified) or their_last_modified > our_last_modified:
        LOGGER.info("Writing fixtures to %s/%s", S3_BUCKET, S3_KEY)
        s3.Object(S3_BUCKET, S3_KEY).put(Body=latest_fixtures.text)
        result = "Fixtures updated!"

    return {
        'statusCode': 200,
        'body': "{}".format(result)
    }


def _get_last_modified(bucket, key):
    last_modified = None
    our_fixtures = s3.Object(bucket, key)
    try:
        our_fixtures.load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['403', '404']:
            # The object does not exist.
            LOGGER.info("%s/%s does not exist", bucket, key)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist
        last_modified = our_fixtures.last_modified

    return last_modified
