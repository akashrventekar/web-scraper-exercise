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
bucket = 'big-data-set'
files_metadata_file_name = 'files_metadata.json'
try:
    files_metadata = s3_client.get_object(
        Bucket=bucket,
        Key=files_metadata_file_name
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

def file_changed(key, link):
    etag_changed_flag, etag = head_request_check_cache(key, link)
    if etag_changed_flag:
        s3_put_response = download_content_upload_file_to_s3(key, link)
        files[key] = (
            etag,
            s3_put_response['ResponseMetadata']['HTTPHeaders']['etag']  # Just storing this in case this is required
        )
        return True
    return False

def head_request_check_cache(key, link):
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
        Bucket=bucket,
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
        file_changed_flag = file_changed(key, link)
    # I am using S3 to store the metadata for now and will recreate this everytime a file is uploaded/modified. However, we can use DynamoDB to store this metadata.
    # Also, store this in another place in case of clashing names
    key = "population_data.json"
    link = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
    file_changed_flag_population_data = file_changed(key, link)

    if file_changed_flag or file_changed_flag_population_data:
        put_response = s3_client.put_object(
            Body=json.dumps(files).encode(),
            Bucket=bucket,
            Key=files_metadata_file_name
        )
        logger.info(f"Recreating metadata file: {put_response}")



    return {"message": "success"}





lambda_handler(None, None)
