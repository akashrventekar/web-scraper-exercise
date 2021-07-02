import requests
from bs4 import BeautifulSoup
import boto3
from botocore.exceptions import ClientError
from collections import defaultdict
import json
import logging as logger
import traceback

s3_client = boto3.client('s3')
files = defaultdict()
try:
    files_metadata = s3_client.get_object(
        Bucket='big-data-set',
        Key='files_metadata'
    )['Body']

    files = json.load(files_metadata)
except ClientError as ex:
    if ex.response['Error']['Code'] == 'NoSuchKey':
        logger.warning('No files metadata found. Downloading all files')
        traceback.print_exc()


def scrape(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    return soup


def get_all_dataset_links(soup):
    results = soup.find_all('a')
    for a in results[1:]:
        yield f"https://download.bls.gov{a['href']}"


def lambda_handler(event, context):
    URL = 'https://download.bls.gov/pub/time.series/pr/'
    soup = scrape(url=URL)
    files_changed = False
    for link in get_all_dataset_links(soup=soup):
        logger.debug(f"Downloading {link}")
        key = link.split("/")[-1]
        external_head_response = requests.head(link)
        # This condition takes care of new data as well as etag being the same. However, breaks readability
        if external_head_response.headers['ETag'] != files.get(key, [None])[0]:
            external_get_response = requests.get(link)

            put_response = s3_client.put_object(
                Body=external_get_response.content,
                Bucket='big-data-set',
                Key=key
            )

            files[key] = (
                external_head_response.headers['ETag'],
                put_response['ResponseMetadata']['HTTPHeaders']['etag'] # Just storing this in case this is required
            )
            files_changed = True
    # I am using S3 to store the metadata for now and will recreate this everytime a file is uploaded/modified. However, we can use DynamoDB to store this metadata.
    # Also, store this in another place in case of clashing names
    if files_changed:
        put_response = s3_client.put_object(
            Body=json.dumps(files).encode(),
            Bucket='big-data-set',
            Key='files_metadata'
        )
        logger.info(f"Recreating metadata file: {put_response}")


    return {"message": "success"}

lambda_handler(None, None)
