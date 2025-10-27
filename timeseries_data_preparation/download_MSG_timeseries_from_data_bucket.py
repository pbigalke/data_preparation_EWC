# script to upload MSG timeseries data to the data bucket
# %%
import os
import boto3
from botocore.exceptions import ClientError
from s3_bucket_credentials import S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL

BUCKETS = ["expats-msg-training", 'expats-random-msg-timeseries-100pix-8frames']

s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_ACCESS_KEY
)

# %%
S3_BUCKET_NAME = BUCKETS[1]

outpath = '/data/crops/dcv2_ir108_100x100_1k_clips_8frame/nc/1'
# os.makedirs(outpath, exist_ok=True)
years = range(2013, 2024)
months = range(4, 10)
days = range(1, 32)
download = False

n_total = 0
for year in years:
    for month in months:
        for day in days:
            print(year, month, day)

            if S3_BUCKET_NAME == 'expats-random-msg-timeseries-100pix-8frames':
                prefix = f"output/data/timeseries_crops/{year:04d}/{month:02d}/{day:02d}/MSG_timeseries_{year:04d}-{month:02d}-{day:02d}_"
            elif S3_BUCKET_NAME == 'expats-msg-training':
                prefix = "TODO"  # Update with the correct prefix if needed
                
            try:
                response = s3.list_objects_v2(
                    Bucket=S3_BUCKET_NAME,
                    Prefix=prefix
                )
                if "Contents" not in response:
                    print(f"No files for {year}-{month:02d}-{day:02d}")
                    continue

                n_day = 0
                for obj in response["Contents"]:
                    key = obj["Key"]
                    if not key.endswith(".nc"):
                        continue
                    n_total += 1
                    n_day += 1

                    if download:
                        filename = os.path.basename(key)
                        local_file = os.path.join(outpath, filename)
                        if os.path.exists(local_file):
                            print(f"Already downloaded: {filename}")
                            continue
                        print(f"Downloading: {key}")
                        with open(local_file, "wb") as f:
                            s3.download_fileobj(S3_BUCKET_NAME, key, f)
            except ClientError as e:
                print(f"Failed to list/download files for {year}-{month:02d}-{day:02d}: {e}")
            print("n_day =", n_day)
print("\nn_total =", n_total)

# %%
