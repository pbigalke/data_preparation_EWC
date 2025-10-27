# methods to read and write data from and to S3 buckets

# %%
import boto3
import os
import logging
from botocore.exceptions import ClientError

# %%
# method to initialize the S3 client
def Initialize_s3_client(S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY):
    """Initialize the S3 client
    :param S3_ENDPOINT_URL: S3 endpoint URL
    :param S3_ACCESS_KEY: S3 access key
    :param S3_SECRET_ACCESS_KEY: S3 secret access key
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

def list_objects(s3, S3_BUCKET_NAME):
    # List the objects in our bucket
    response = s3.list_objects(Bucket=S3_BUCKET_NAME)
    for item in response['Contents']:
        print(item['Key'])
    

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
