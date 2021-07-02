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

def files_changed(key, link):
    file_changed_flag, etag = file_changed(key, link)
    if file_changed_flag:
        s3_put_response = download_content_upload_file_to_s3(key, link)
        files[key] = (
            etag,
            s3_put_response['ResponseMetadata']['HTTPHeaders']['etag']  # Just storing this in case this is required
        )
        return True
    return False

def file_changed(key, link):
    external_head_response = requests.head(link)
    # This condition takes care of new data as well as etag being the same. However, breaks readability
    # Also, the If-None-Match header for this server was not working and would always return a 200.
    # I have not used If-Modified-Since for now.

    external_head_response_etag = external_head_response.headers['ETag']

    if external_head_response_etag != files.get(key, [None])[0]:
        return True, external_head_response_etag
    return False, external_head_response_etag

def download_content_upload_file_to_s3(key, link):
    external_get_response = requests.get(link)

    return s3_client.put_object(
        Body=external_get_response.content,
        Bucket='big-data-set',
        Key=key
    )


def lambda_handler(event, context):
    URL = 'https://download.bls.gov/pub/time.series/pr/'
    soup = scrape(url=URL)
    links = get_all_dataset_links(soup=soup)

    files_changed_flag = False
    for link in links:
        logger.debug(f"Downloading {link}")
        key = link.split("/")[-1]
        files_changed_flag = files_changed(key, link)
    # I am using S3 to store the metadata for now and will recreate this everytime a file is uploaded/modified. However, we can use DynamoDB to store this metadata.
    # Also, store this in another place in case of clashing names
    key = "population_data.json"
    link = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
    files_changed_flag_population_data = files_changed(key, link)

    if files_changed_flag or files_changed_flag_population_data:
        put_response = s3_client.put_object(
            Body=json.dumps(files).encode(),
            Bucket='big-data-set',
            Key='files_metadata'
        )
        logger.info(f"Recreating metadata file: {put_response}")



    return {"message": "success"}





lambda_handler(None, None)
