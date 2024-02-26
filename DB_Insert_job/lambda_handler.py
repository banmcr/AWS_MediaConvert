import glob
import bson
import json
import os
import uuid
import boto3
import datetime
import random
from urllib.parse import urlparse
import urllib
import logging
import pymongo


s3 = boto3.resource('s3')
lam= boto3.client('lambda')
aws_region = boto3.session.Session().region_name

def lambda_handler(event, context):
    destS3Bucket = event["detail"]["outputGroupDetails"][0]["playlistFilePaths"][0]
    DS3Bucket=''.join(destS3Bucket.split("/")[2])
    file=destS3Bucket.split("/")[-1]
    name=file.split("_")[0]
    print(name)
    s3_key= '/'.join(destS3Bucket.split("/")[3:])
    s3plus=s3_key.replace(" ","+")
    s3_final=urllib.parse.quote(s3plus,safe="+/")
    print(s3_final)
    

    s3_url= f"https://{DS3Bucket}.s3.{aws_region}.amazonaws.com/{s3_final}"
    print(s3_url)
    #    https://abcccc.s.llnwi.net/in10hls/<Folder>/<Main.m3u8> 
    
    myclient = pymongo.MongoClient("mongodb://<PUT MONGODB IP OR HOST OR URL>:27017/")
    mydb = myclient["testdb"]
    mycol = mydb["customers"]
    x = mycol.find_one()
    print(x)
    myquery = { "id": int(name) }
    newvalues = { "$set": { "video_url": s3_url,"video_status": "complete" } }
    mycol.update_one(myquery, newvalues)