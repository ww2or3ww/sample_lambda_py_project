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
rekognition = boto3.client('rekognition')

from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
ENDPOINT = "https://alcobs3s65as5mboez7swwzlyu.appsync-api.ap-northeast-1.amazonaws.com/graphql"
API_KEY = "da2-berkdxpigfambfqv6hcivejndy"
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
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
    logger.info("Function Start (deploy from S3) : Bucket={0}, Key={1}" .format(bucket, key))

    fileName = os.path.basename(key)
    dirPath = os.path.dirname(key)
    dirName = os.path.basename(dirPath)
    
    orgFilePath = "/tmp/" + fileName
    
    if (not key.startswith("public") or key.startswith("public/processed/")):
        logger.info("don't process.")
        return
    
    apiCreateTable(dirName, key)

    keyOut = key.replace("public", "public/processed", 1)
    dirPathOut = os.path.dirname(keyOut)

    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=orgFilePath)

        orgImage = cv2.imread(orgFilePath)
        grayImage = cv2.cvtColor(orgImage, cv2.COLOR_RGB2GRAY)
        processedFileName = "gray-" + fileName
        processedFilePath = "/tmp/" + processedFileName
        uploadImage(grayImage, processedFilePath, bucket, os.path.join(dirPathOut, processedFileName), dirName)

        detectFaces(bucket, key, fileName, orgImage, dirName, dirPathOut)

    except Exception as e:
        logger.error(e)
        raise e
        
    finally:
        if os.path.exists(orgFilePath):
            os.remove(orgFilePath)

def uploadImage(image, localFilePath, bucket, s3Key, group):
    logger.info("start uploadImage({0}, {1}, {2}, {3})".format(localFilePath, bucket, s3Key, group))
    try:
        cv2.imwrite(localFilePath, image)
        s3.upload_file(Filename=localFilePath, Bucket=bucket, Key=s3Key)
        apiCreateTable(group, s3Key)
    except Exception as e:
        logger.error(e)
        raise e
    finally:
        if os.path.exists(localFilePath):
            os.remove(localFilePath)

def apiCreateTable(group, path):
    logger.info("start apiCreateTable({0}, {1})".format(group, path))
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
        logger.error(e)
        raise e

def detectFaces(bucket, key, fileName, image, group, dirPathOut):
    logger.info("start detectFaces ({0}, {1}, {2}, {3}, {4})".format(bucket, key, fileName, group, dirPathOut))
    try:
        response = rekognition.detect_faces(
            Image={
                "S3Object": {
                    "Bucket": bucket,
                    "Name": key,
                }
            },
            Attributes=[
                "DEFAULT",
            ]
        )
        
        name, ext = os.path.splitext(fileName)
        imgHeight = image.shape[0]
        imgWidth = image.shape[1]
        index = 0
        for faceDetail in response["FaceDetails"]:
            index += 1
            faceFileName = "face_{0:03d}".format(index) + ext
            box = faceDetail["BoundingBox"]
            x = max(int(imgWidth * box["Left"]), 0)
            y = max(int(imgHeight * box["Top"]), 0)
            w = int(imgWidth * box["Width"])
            h = int(imgHeight * box["Height"])
            logger.info("BoundingBox({0},{1},{2},{3})".format(x, y, w, h))
            
            faceImage = image[y:min(y+h, imgHeight-1), x:min(x+w, imgWidth)]
            
            localFaceFilePath = os.path.join("/tmp/", faceFileName)
            uploadImage(faceImage, localFaceFilePath, bucket, os.path.join(dirPathOut, faceFileName), group)
            cv2.rectangle(image, (x, y), (x+w, y+h), (0, 0, 255), 3)

        processedFileName = "faces-" + fileName
        processedFilePath = "/tmp/" + processedFileName
        uploadImage(image, processedFilePath, bucket, os.path.join(dirPathOut, processedFileName), group)
    except Exception as e:
        logger.error(e)
