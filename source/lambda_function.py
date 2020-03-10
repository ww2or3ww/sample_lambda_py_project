# coding: UTF-8
import boto3
import os
import datetime
import json
from urllib.parse import unquote_plus
import numpy as np
import cv2
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
rekognition = boto3.client("rekognition")


# API_KEY = "da2-zcpxv5dkprh7lnz7sqfsqx5jnu"
# _headers = {
#     "Content-Type": "application/graphql",
#     "x-api-key": API_KEY,
# }

import requests
from requests_aws4auth import AWS4Auth

AWS_REGION = os.environ["AWS_REGION"]
AWS_ACCESS_KEY_ID = os.environ["AWS_ACCESS_KEY_ID"]
AWS_SECRET_ACCESS_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
AWS_SESSION_TOKEN = os.environ["AWS_SESSION_TOKEN"]
ENDPOINT = "https://{0}.{1}.{2}.amazonaws.com/{3}".format("alcobs3s65as5mboez7swwzlyu", "appsync-api", AWS_REGION, "graphql")
AUTH = AWS4Auth(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, 'appsync', session_token=AWS_SESSION_TOKEN)

logger.info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

from base64 import b64encode
from googleapiclient.discovery import build 
from googleapiclient.http import MediaFileUpload 
from oauth2client.service_account import ServiceAccountCredentials 

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")
    logger.info("Function Start (deploy from S3) : Bucket={0}, Key={1}".format(bucket, key))

    fileName = os.path.basename(key)
    dirPath = os.path.dirname(key)
    dirName = os.path.basename(dirPath)
    pathList = key.split("/")
    
    orgFilePath = "/tmp/" + fileName
    
    logger.info("START XXX ({0}, {1})".format(pathList[0], len(pathList)))
    
    if (not pathList[0] == "protected"  or len(pathList) != 4):
        logger.info("don't process.")
        return

    apiCreateTable(dirName, os.path.join(pathList[2], pathList[3]), "-")

    keyOut = os.path.join(pathList[0], pathList[1], "processed", pathList[2], pathList[3])
    dirPathOut = os.path.dirname(keyOut)
    dirPathOut2 = os.path.join("processed", pathList[2])

    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=orgFilePath)

        orgImage = cv2.imread(orgFilePath)
        grayImage = cv2.cvtColor(orgImage, cv2.COLOR_RGB2GRAY)
        processedFileName = "gray-" + fileName
        processedFilePath = "/tmp/" + processedFileName
        uploadImage(grayImage, processedFilePath, bucket, os.path.join(dirPathOut, processedFileName), os.path.join(dirPathOut2, processedFileName), dirName, "-", False)

        fileID = uploadFileToGoogleDrive(key, orgFilePath)
        detectFaces(bucket, key, fileName, orgImage, dirName, dirPathOut, dirPathOut2)
        # detectFacesByGoogleVisionAPI(fileID, bucket, dirPathOut)
        # detectFacesByGoogleVisionAPIFromF(bucket, orgFilePath, orgImage, dirName, dirPathOut)

        processedFileName = "faces-" + fileName
        processedFilePath = "/tmp/" + processedFileName
        uploadImage(orgImage, processedFilePath, bucket, os.path.join(dirPathOut, processedFileName), os.path.join(dirPathOut2, processedFileName), dirName, "-", True)

    except Exception as e:
        logger.exception(e)
        raise e
        
    finally:
        if os.path.exists(orgFilePath):
            os.remove(orgFilePath)

def uploadImage(image, localFilePath, bucket, s3Key, path, group, points, isUploadGoogleDrive):
    logger.info("start uploadImage({0}, {1}, {2}, {3}, {4})".format(localFilePath, bucket, s3Key, path, group))
    try:
        cv2.imwrite(localFilePath, image)
        s3.upload_file(Filename=localFilePath, Bucket=bucket, Key=s3Key)
        apiCreateTable(group, path, points)
        if isUploadGoogleDrive:
            uploadFileToGoogleDrive(s3Key, localFilePath)
    except Exception as e:
        logger.exception(e)
        raise e
    finally:
        if os.path.exists(localFilePath):
            os.remove(localFilePath)

def apiCreateTable(group, path, points):
    logger.info("start apiCreateTable({0}, {1}, {2})".format(group, path, points))

    time = datetime.datetime.now()
    time = time + datetime.timedelta(minutes=30)
    epocTime = int(time.timestamp())

    try:
#        query = gql("""
#            mutation create {{
#                createSampleAppsyncTable(input:{{
#                group: \"{0}\"
#                path: \"{1}\"
#                deleteTime: {2}
#              }}){{
#                group path
#              }}
#            }}
#            """.format(group, path, epocTime))
        # _client.execute(query)
        
        body_json = {"query": 
            """
            mutation create {{
                createSampleAppsyncTable(input:{{
                group: \"{0}\"
                path: \"{1}\"
                points: \"{2}\"
                deleteTime: {3}
              }}){{
                group path points
              }}
            }}
            """.format(group, path, points, epocTime)
        }
        body = json.dumps(body_json)
        # headers = {}
        logger.info(">>>>>>>request begin>>>>>>>>>>>>>>")
        logger.info(body)
        response = requests.request("POST", ENDPOINT, auth=AUTH, data=body, headers={})
        logger.info(response)
        logger.info("<<<<<<<request end<<<<<<<<<<<<<<<<")
        
    except Exception as e:
        logger.exception(e)
        raise e

def detectFaces(bucket, key, fileName, image, group, dirPathOut, dirPathOut2):
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
        
        jsonFileName = name + ".json"
        uploadJsonToS3(jsonFileName, response, bucket, dirPathOut)
        
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
            points = "{0},{1}|{2},{3}|{4},{5}|{6},{7}".format(x, y, x+w, y, x+w, y+h, x, y+h)
            logger.info("BoundingBox({0},{1},{2},{3})".format(x, y, w, h))
            
            faceImage = image[y:min(y+h, imgHeight-1), x:min(x+w, imgWidth)]
            
            localFaceFilePath = os.path.join("/tmp/", faceFileName)
            uploadImage(faceImage, localFaceFilePath, bucket, os.path.join(dirPathOut, faceFileName), os.path.join(dirPathOut2, faceFileName), group, points, False)
            cv2.rectangle(image, (x, y), (x+w, y+h), (0, 0, 255), 3)

    except Exception as e:
        logger.exception(e)
        raise e

def uploadJsonToS3(jsonFileName, jsondata, bucket, dirPathOut):
    try:
        localPathJSON = "/tmp/" + jsonFileName
        with open(localPathJSON, 'w') as f:
            json.dump(jsondata, f, ensure_ascii=False)
            
        s3.upload_file(Filename=localPathJSON, Bucket=bucket, Key=os.path.join(dirPathOut, jsonFileName))
        if os.path.exists(localPathJSON):
            os.remove(localPathJSON)

    except Exception as e:
        logger.exception(e)
        raise e

def detectFacesByGoogleVisionAPIFromF(bucket, localFilePath, image, group, dirPathOut, dirPathOut2):
    try:
        keyFile = "service-account-vision-key.json"
        scope = ["https://www.googleapis.com/auth/cloud-vision"]
        api_name = "vision"
        api_version = "v1"
        service = getGoogleService(keyFile, scope, api_name, api_version)
        
        ctxt = None
        with open(localFilePath, 'rb') as f:
            ctxt = b64encode(f.read()).decode()
        
        service_request = service.images().annotate(body={
            "requests": [{
                "image":{
                    "content": ctxt
                  },
                "features": [
                    {
                        "type": "FACE_DETECTION"
                    }, 
                    {
                        "type": "TEXT_DETECTION"
                    }
                ]
            }]
        })
        response = service_request.execute()
        uploadJsonToS3("visionResult.json", response, bucket, dirPathOut)
        
        fullTextAnnotation = response["responses"][0]["fullTextAnnotation"]
        blocks = fullTextAnnotation["pages"][0]["blocks"]

        name, ext = os.path.splitext(localFilePath)
        imgHeight = image.shape[0]
        imgWidth = image.shape[1]
        index = 0
        for block in blocks:
            index += 1
            fileName = "word_{0:03d}".format(index) + ext
            box = block["boundingBox"]
            logger.info("boundingBox : {0}".format(box))
            x = min(box["vertices"][0]["x"], box["vertices"][1]["x"], box["vertices"][2]["x"], box["vertices"][3]["x"])
            y = min(box["vertices"][0]["y"], box["vertices"][1]["y"], box["vertices"][2]["y"], box["vertices"][3]["y"])
            w = max(box["vertices"][0]["x"], box["vertices"][1]["x"], box["vertices"][2]["x"], box["vertices"][3]["x"]) - x
            h = max(box["vertices"][0]["y"], box["vertices"][1]["y"], box["vertices"][2]["y"], box["vertices"][3]["y"]) - y
            points = "{0},{1}|{2},{3}|{4},{5}|{6},{7}".format(x, y, x+w, y, x+w, y+h, x, y+h)
            logger.info("BoundingBox({0},{1},{2},{3})".format(x, y, w, h))
            
            faceImage = image[y:min(y+h, imgHeight-1), x:min(x+w, imgWidth)]
            
            localFaceFilePath = os.path.join("/tmp/", fileName)
            uploadImage(faceImage, localFaceFilePath, bucket, os.path.join(dirPathOut, fileName), os.path.join(dirPathOut2, fileName), group, points, False)
            cv2.rectangle(image, (x, y), (x+w, y+h), (0, 255, 0), 3)

    except Exception as e:
        logger.exception(e)

def detectFacesByGoogleVisionAPI(fileID, bucket, dirPathOut):
    try:
        # https://drive.google.com/uc?export=view&id=1UqOiATi2Wcvz0pD4prJAbeSg81LiUP4l
        # fileID = "1UqOiATi2Wcvz0pD4prJAbeSg81LiUP4l"
        
        # keyFile = "service-account-vision-key.json"
        keyFile = "service-account-key.json"
        scope = ["https://www.googleapis.com/auth/cloud-vision"]
        api_name = "vision"
        api_version = "v1"
        service = getGoogleService(keyFile, scope, api_name, api_version)
        
        # imageUrl = "https://drive.google.com/open?id=" + fileID
        imageUrl = "https://drive.google.com/uc?id=" + fileID
        # imageUrl = "https://mosaic-site-production.s3-ap-northeast-1.amazonaws.com/img/Screenshot+2020-01-11+at+17.11.11.png"
        # imageUrl = "https://doc-0o-bg-docs.googleusercontent.com/docs/securesc/ha0ro937gcuc7l7deffksulhg5h7mbp1/4knjdgjo49ge8540vccmb59ao59p45b4/1578780000000/14154867416249163296/*/1RFCadzzl9vus6Itv1auT2Tl2Q3SyZekn"
        
        logger.info("imageUrl = {0}".format(imageUrl))

        service_request = service.images().annotate(body={
            "requests": [{
                "image":{
                    "source":{
                      "imageUri": imageUrl
                    }
                  },
                "features": [
                    {
                        "type": "FACE_DETECTION"
                    }, 
                    {
                        "type": "TEXT_DETECTION"
                    }
                ]
            }]
        })
        response = service_request.execute()
        
        logger.info(response)
        
        uploadJsonToS3("visionResult.json", response, bucket, dirPathOut)
        
    except Exception as e:
        logger.exception(e)

def uploadFileToGoogleDrive(fileName, localFilePath):
    try:
        ext = os.path.splitext(localFilePath.lower())[1][1:]
        if ext == "jpg":
            ext = "jpeg"
        mimeType = "image/" + ext

        keyFile = "service-account-key.json"
        scope = ['https://www.googleapis.com/auth/drive.file'] 
        api_name = "drive"
        api_version = "v3"
        service = getGoogleService(keyFile, scope, api_name, api_version)
        file_metadata = {"name": fileName, "mimeType": mimeType, "parents": ["1gPIeYJgNdYc9ETa_GXLxOuox1PSZQe7O"] } 
        media = MediaFileUpload(localFilePath, mimetype=mimeType, resumable=True) 
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        logger.info("uploaded : {0}".format(file))
        return file["id"]

    except Exception as e:
        logger.exception(e)

def getGoogleService(keyFile, scope, api_name, api_version):
    credentialsG = ServiceAccountCredentials.from_json_keyfile_name(keyFile, scopes=scope)
    return build(api_name, api_version, credentials=credentialsG, cache_discovery=False) 


