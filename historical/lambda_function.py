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

    # e.g. "https://www.football-data.co.uk/mmz4281/"
    BASE_URL = os.environ['BASE_URL']
    LOGGER.info('BASE_URL: %s', BASE_URL)

    # e.g. "2021"
    LATEST_SEASON = os.environ['LATEST_SEASON']
    LOGGER.info('LATEST_SEASON: %s', LATEST_SEASON)

    # e.g. "1819,1920"
    PREVIOUS_SEASONS = os.environ['PREVIOUS_SEASONS']
    LOGGER.info('PREVIOUS_SEASONS: %s', PREVIOUS_SEASONS)

    # e.g. "E0, D1, SP1"
    LEAGUES = os.environ['LEAGUES']
    LOGGER.info('LEAGUES: %s', LEAGUES)

    S3_BUCKET = os.environ['S3_BUCKET']
    LOGGER.info('S3_BUCKET: %s', S3_BUCKET)

    S3_PREFIX = os.environ['S3_PREFIX']
    LOGGER.info('S3_PREFIX: %s', S3_PREFIX)


    n_updates = 0

    # Check each league for updates and overwrite if necessary
    for league in [l.strip() for l in LEAGUES.split(',')]:

        # Check last-modified time of our copy of the file
        s3object = _get_s3_object(S3_BUCKET, S3_PREFIX, league)
        our_last_modified = _get_last_modified(s3object)

        # Download the data for the latest season
        latest_season = _download_data(BASE_URL, LATEST_SEASON, league)

        # Compare last-modified dates; if latest_season is not newer, then nothing to do
        # (Not sure if I'm doing the right thing here with UTC -- prob. should be BST)
        their_last_modified = datetime.strptime(
            latest_season.headers['Last-Modified'], "%a, %d %b %Y %H:%M:%S %Z").astimezone(timezone.utc)
        LOGGER.info("%s last modified: %s", latest_season.url, their_last_modified)

        if our_last_modified and our_last_modified >= their_last_modified:
            LOGGER.info("No changes for %s.", league)
            continue

        n_updates += 1

        # Get the previous seasons' data, combine it with latest season's data, and write to S3
        data = ''
        for season in sorted([s.strip() for s in PREVIOUS_SEASONS.split(',')]):
            past_season = _download_data(BASE_URL, season, league)
            data += past_season.text
        data += latest_season.text

        LOGGER.info("Writing data to %s/%s/%s.csv", S3_BUCKET, S3_PREFIX, league)
        s3object.put(Body=data)

    return {
        'statusCode': 200,
        'body': "Number of updates: {}".format(n_updates)
    }


def _get_last_modified(s3object):
    last_modified = None
    try:
        s3object.load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] in ['403', '404']:
            # The object does not exist.
            LOGGER.info("%s/%s does not exist", s3object.bucket_name, s3object.key)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The object does exist
        last_modified = s3object.last_modified

    LOGGER.info("%s/%s last modified: %s" , s3object.bucket_name, s3object.key, last_modified)
    return last_modified


def _get_s3_object(bucket, prefix, league):
    key = prefix.rstrip('/') + '/' + league + '.csv'
    return s3.Object(bucket, key)


def _download_data(base, season, league):
    # e.g https://www.football-data.co.uk/mmz4281/2021/E0.csv
    url = base.rstrip('/') + '/' + season + '/' + league + '.csv'
    response = requests.get(url)
    response.raise_for_status()
    if response.status_code != 200:
        raise Exception("Can't download from %s: status_code=%d, reason=%s" %
                        (url, response.status_code, response.reason))
    return response
