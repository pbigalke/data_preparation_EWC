# script to upload MSG timeseries data to the data bucket

# %%
import time
from glob import glob

from s3_bucket_credentials import S3_BUCKET_TIMESERIES_NAME, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL
from data_buckets_read_and_write import Initialize_s3_client, upload_file

# %%
#Directory with the data to upload
years = range(2013, 2024)
months = range(4, 10)
days = range(1, 32) #[9, 10, 11]
path_to_data = "output/data/timeseries_crops"

# initialize the S3 client to upload the data to bucket
s3 = Initialize_s3_client(S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY)

# %%
# Upload the data to the bucket
start_time = time.time()
total = 0
for year in years:
    print()
    print("Year: ", year, flush=True)
    for month in months:
        count_month = 0
        for day in days:
            data_filepattern = f"{path_to_data}/{year:04d}/{month:02d}/{day:02d}/*.nc"
            file_list = sorted(glob(data_filepattern))
            if len(file_list) > 0:
                count_month += len(file_list)

                for file in file_list:
                    #Uploading a file to the bucket (make sure you have write access)
                    #file_size = os.path.getsize(file)  # Get file size in bytes
                    # Open file in binary mode and upload
                    upload_file(s3, file, S3_BUCKET_TIMESERIES_NAME, file)

        print("Month: ", month, " files found: ", count_month, flush=True)	
        total += count_month
print("Total files to upload: ", total, flush=True)   
print("Time taken to upload files: ", time.time() - start_time, flush=True) 

# %%
# # List the objects in our bucket to check if the files were uploaded
# response = s3.list_objects(Bucket=S3_BUCKET_TIMESERIES_NAME)
# n = 0
# for item in response['Contents']:
#     # print(item['Key'])
#     n += 1
# print("Total files in the bucket: ", n)
# %%
