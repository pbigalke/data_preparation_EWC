# methods to read and write data from and to S3 buckets

# %%
import boto3
import os
import logging
from botocore.exceptions import ClientError
from data_buckets_IO.bucket_information import get_bucket_prefix
from data_buckets_IO.s3_bucket_credentials import S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

# %%
# method to initialize the S3 client
def Initialize_s3_client():
    """Initialize the S3 client
    :return: S3 client object
    """
    # Initialize the S3 client
    s3 = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY
    )
    return s3

# %%
# methods for reading data
def read_file(s3, file_name, bucket):
    """reading a file from an S3 bucket
    :param s3: Initialized S3 client object
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :return: object if file was uploaded, else False
    """
    try:
        #with open(file_name, "rb") as f:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        #print(obj)
        myObject = obj['Body'].read().decode('utf-8')
    except ClientError as e:
        logging.error(e)
        return None
    return myObject

def download_file(s3, file_name, bucket, local_path):
    """Download a file from an S3 bucket

    :param s3: Initialized S3 client object
    :param file_name: File to download
    :param bucket: Bucket to download from
    :param local_path: Local path to save the downloaded file
    :return: True if file was downloaded, else False
    """

    try:
        with open(local_path, "wb") as f:
            s3.download_fileobj(bucket, file_name, f)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def list_objects(s3, S3_BUCKET_NAME):
    # List the objects in our bucket
    response = s3.list_objects(Bucket=S3_BUCKET_NAME)
    for item in response['Contents']:
        print(item['Key'])
    
def list_objects_within_study_period(s3, S3_BUCKET_NAME, years, months, days):
    """List all object names within the study period"""
    
    all_files = []

    for year in years:
        for month in months:
            for day in days:
                
                # get prefix for the folder structure in the bucket
                prefix = get_bucket_prefix(S3_BUCKET_NAME, year, month, day)
                    
                # read all objects for the given day
                response = s3.list_objects_v2(
                    Bucket=S3_BUCKET_NAME,
                    Prefix=prefix
                )
                if "Contents" not in response:
                    continue

                # loop over all objects for the given day
                for obj in response["Contents"]:
                    key = obj["Key"]
                    if not key.endswith(".nc"):
                        continue
                    
                    all_files.append(key)
    
    return all_files

# %%
# method to upload data
def upload_file(s3_client, file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    try:
        with open(file_name, "rb") as f:
            s3_client.upload_fileobj(f, bucket, object_name)
        #response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
# %%
