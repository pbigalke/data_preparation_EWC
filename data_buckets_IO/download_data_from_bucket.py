# script to upload MSG timeseries data to the data bucket
# %%
import os
import boto3
from botocore.exceptions import ClientError
from s3_bucket_credentials import S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

BUCKETS = ["expats-msg-training", 'expats-random-msg-timeseries-100pix-8frames', 'mwcch-hail-regrid-msg']

def get_bucket_prefix(bucket_name, year, month, day):
    """Get the prefix for the given bucket and date
    :param bucket_name: Name of the S3 bucket
    :param year: Year of the data
    :param month: Month of the data
    :param day: Day of the data
    :return: Prefix for the S3 objects
    """
    if bucket_name == 'expats-random-msg-timeseries-100pix-8frames':
        return f"output/data/timeseries_crops/{year:04d}/{month:02d}/{day:02d}/MSG_timeseries_{year:04d}-{month:02d}-{day:02d}_"
    
    elif bucket_name == 'mwcch-hail-regrid-msg':
        return f"{year:04d}/{month:02d}/{day:02d}/{year:04d}{month:02d}{day:02d}_"
    
    return None


s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY
)

# %%
S3_BUCKET_NAME = 'mwcch-hail-regrid-msg'

outpath = None #'/data/crops/dcv2_ir108_100x100_1k_clips_8frame/nc/1'
# os.makedirs(outpath, exist_ok=True)
years = range(2006, 2024)
months = range(4, 10)
days = range(1, 32)
download = False
verbose = False

n_total = 0

for year in years:
    n_year = 0

    for month in months:
        n_month = 0

        for day in days:
            
            # get prefix for the folder structure in the bucket
            prefix = get_bucket_prefix(S3_BUCKET_NAME, year, month, day)
                
            try:
                # read all objects for the given day
                response = s3.list_objects_v2(
                    Bucket=S3_BUCKET_NAME,
                    Prefix=prefix
                )
                if "Contents" not in response:
                    continue

                n_day = 0
                # loop over all objects for the given day
                for obj in response["Contents"]:
                    key = obj["Key"]
                    if not key.endswith(".nc"):
                        continue
                    n_day += 1
                    n_month += 1

                    if download and outpath is not None:
                        # get filename of the object
                        filename = os.path.basename(key)

                        # define local path to save the file
                        local_file = os.path.join(outpath, filename)

                        # check if file already exists
                        if os.path.exists(local_file):
                            if verbose:
                                print(f"Already downloaded: {filename}")
                            continue

                        # download file to local path
                        if verbose:
                            print(f"Downloading: {key}")
                        with open(local_file, "wb") as f:
                            s3.download_fileobj(S3_BUCKET_NAME, key, f)
            
            # catching errors
            except ClientError as e:
                print(f"Failed to list/download files for {year}-{month:02d}-{day:02d}: {e}")
            
            if verbose:
                print(f">>> {year}{month:02d}{day:02d}: {n_day} files")

        n_year += n_month
        print(f"> {year}{month:02d}: {n_month} files")
    
    n_total += n_year
    print(f"{year}: {n_year} files")

print("\nn_total =", n_total)

# %%
