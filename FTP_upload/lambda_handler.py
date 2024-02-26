import os
import json
from ftplib import FTP
import boto3

# Source https://github.com/Vibish/FTP_SFTP_LAMBDA/blob/master/FTPThroughLambda.py
# https://www.edureka.co/community/17558/python-aws-boto3-how-do-i-read-files-from-s3-bucket
# https://medium.com/better-programming/transfer-file-from-ftp-server-to-a-s3-bucket-using-python-7f9e51f44e35
# https://github.com/kirankumbhar/File-Transfer-FTP-to-S3-Python/blob/master/ftp_to_s3.py
# https://dashbird.io/blog/python-aws-lambda-error-handling/


# For example: FTP_HOST = ftp.your_ftp_host.com
FTP_HOST = 'abc.upload.llnw.net'
FTP_USER = 'asdsadsadia-vs'
FTP_PWD = 'asdsadsad'
# For example: FTP_PATH = '/home/logs/'

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

def lambda_handler(event, context):
    destS3Bucket = event["detail"]["outputGroupDetails"][0]["playlistFilePaths"][0]
    DS3Bucket=''.join(destS3Bucket.split("/")[2])
    print(DS3Bucket)
    S3Dir=''.join(destS3Bucket.split("/")[3])
    print(S3Dir)
    s3path=r's3://'+DS3Bucket+'/'+S3Dir+''
    print(s3path)
    
    FTP_PATH = '/'+S3Dir+'/'
    print(FTP_PATH)
    
    prefixitem=str(""+S3Dir+"/")
    my_bucket = s3.Bucket(DS3Bucket)
    print(my_bucket)
   
    for object_summary in my_bucket.objects.filter(Prefix=prefixitem):
        print(object_summary.key)
        sourcebucket=DS3Bucket
        sourcekey=object_summary.key
        filename = os.path.basename(sourcekey)
        download_path = '/tmp/'+ filename
        print(download_path)
        s3_client.download_file(sourcebucket, sourcekey, download_path)
        os.chdir("/tmp/")
        
        with FTP(FTP_HOST, FTP_USER, FTP_PWD) as ftp, open(filename, 'rb') as file:
                ftp.storbinary(f'STOR {FTP_PATH}{file.name}', file)
                
        os.remove(filename)
    
    uploadThis(s3path)

    if event and event['Records']:
        for record in event['Records']:
            sourcebucket = record['s3']['bucket']['name']
            sourcekey = record['s3']['object']['key']
            
            #Download the file to /tmp/ folder
            filename = os.path.basename(sourcekey)
            download_path = '/tmp/'+ filename
            print(download_path)
            s3_client.download_file(sourcebucket, sourcekey, download_path)
            
            os.chdir("/tmp/")
            with FTP(FTP_HOST, FTP_USER, FTP_PWD) as ftp, open(filename, 'rb') as file:
                ftp.storbinary(f'STOR {FTP_PATH}{file.name}', file)

            #We don't need the file in /tmp/ folder anymore
            os.remove(filename)
def uploadThis(path):
    files = os.listdir(path)
    os.chdir(path)
    for f in files:
        if os.path.isfile(path + r'\{}'.format(f)):
            fh = open(f, 'rb')
            myFTP.storbinary('STOR %s' % f, fh)
            fh.close()
        elif os.path.isdir(path + r'\{}'.format(f)):
            myFTP.mkd(f)
            myFTP.cwd(f)
            uploadThis(path + r'\{}'.format(f))
    myFTP.cwd('..')
    os.chdir('..')