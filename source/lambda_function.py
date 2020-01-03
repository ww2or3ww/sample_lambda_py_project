# coding: UTF-8
import boto3
import os
from urllib.parse import unquote_plus
import numpy as np
import cv2
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
ENDPOINT = "https://**********.appsync-api.ap-northeast-1.amazonaws.com/graphql"
API_KEY = "da2-**********"
_headers = {
    "Content-Type": "application/graphql",
    "x-api-key": API_KEY,
}
_transport = RequestsHTTPTransport(
    headers = _headers,
    url = ENDPOINT,
    use_json = True,
)
_client = Client(
    transport = _transport,
    fetch_schema_from_transport = True,
)

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info("Function Start (deploy from S3) : Bucket={0}, Key={1}" .format(bucket, key))

    fileName = os.path.basename(key)
    dirPath = os.path.dirname(key)
    dirName = os.path.basename(dirPath)
    
    orgFilePath = u'/tmp/' + fileName
    processedFilePath = u'/tmp/processed-' + fileName

    if (key.startswith("public/processed/")):
        logger.info("not start with public")
        return
    
    apiCreateTable(dirName, key)

    keyOut = key.replace("public", "public/processed", 1)
    logger.info("Output local path = {0}".format(processedFilePath))

    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=orgFilePath)

        orgImage = cv2.imread(orgFilePath)
        grayImage = cv2.cvtColor(orgImage, cv2.COLOR_RGB2GRAY)
        cv2.imwrite(processedFilePath, grayImage)

        s3.upload_file(Filename=processedFilePath, Bucket=bucket, Key=keyOut)
        apiCreateTable(dirName, keyOut)

        logger.info("Function Completed : processed key = {0}".format(keyOut))

    except Exception as e:
        print(e)
        raise e
        
    finally:
        if os.path.exists(orgFilePath):
            os.remove(orgFilePath)
        if os.path.exists(processedFilePath):
            os.remove(processedFilePath)

def apiCreateTable(group, path):
    logger.info("group={0}, path={1}".format(group, path))
    try:
        query = gql("""
            mutation create {{
                createSampleAppsyncTable(input:{{
                group: \"{0}\"
                path: \"{1}\"
              }}){{
                group path
              }}
            }}
            """.format(group, path))
        _client.execute(query)
    except Exception as e:
        print(e)
