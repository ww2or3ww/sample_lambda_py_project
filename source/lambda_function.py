# coding: UTF-8
import boto3
import os
import re
import json
import datetime
from urllib.parse import unquote_plus

import numpy as np
import cv2
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def lambda_handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    logger.info("Function Start (deploy from S3) : Bucket={0}, Key={1}" .format(bucket, key))

    fileName = os.path.basename(key)
    localTmpPath = u'/tmp/' + fileName
    localTmpPath2 = u'/tmp2/' + fileName

    if (not key.startswith("public")):
        logger.info("not start with public")
        return

    keyOut = key.replace("public", "processed", 1)
    logger.info("Output key = {0}".format(keyOut))

    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=localTmpPath)
        
        before = cv2.imread(localTmpPath)
        gray = cv2.cvtColor(before, cv2.COLOR_RGB2GRAY)
        
        cv2.imwrite(localTmpPath2, gray)
        s3.upload_file(Filename=localTmpPath2, Bucket=bucket, Key=keyOut)

        logger.info("process completed !!")
    except Exception as e:
        print(e)
        raise e
        
    finally:
        if os.path.exists(localTmpPath):
            os.remove(localTmpPath)
        if os.path.exists(localTmpPath2):
            os.remove(localTmpPath2)
