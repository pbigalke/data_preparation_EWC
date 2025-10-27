# script to upload MSG timeseries data to the data bucket

# %%
import time
from glob import glob
import os
import shutil
import tarfile
from paramiko import SSHClient
from scp import SCPClient
from s3_bucket_credentials import S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY, S3_ENDPOINT_URL
from data_buckets_read_and_write import Initialize_s3_client, upload_file

def ssh_scp_files(ssh_host, ssh_user, ssh_password, ssh_port, source_volume, destination_volume):
    logging.info("In ssh_scp_files()method, to copy the files to the server")
    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.connect(ssh_host, username=ssh_user, password=ssh_password, look_for_keys=False)

    with SCPClient(ssh.get_transport()) as scp:
        scp.get(source_volume, recursive=True, remote_path=destination_volume)

# %%
#Directory with the data to upload
years = range(2017, 2025)
months = range(4, 10)
days = range(1, 32) #[9, 10, 11]
path = "/data/EUCLID/all_years_tar_files"
BUCKET_NAME = "expats-euclid"
delete_extracted = True
delete_tar_after_upload = True

# initialize the S3 client to upload the data to bucket
s3 = Initialize_s3_client(S3_ENDPOINT_URL, S3_ACCESS_KEY, S3_SECRET_ACCESS_KEY)

# %%
# Upload the data to the bucket
start_time = time.time()
total = 0
for year in years:
    print()
    print("Year: ", year, flush=True)

    remote_tar_file = f'/net/merisi/pbigalke/data/EUCLID/all_years_tar_files/{year}.tar'
    year_tar_file = f"{path}/{year}.tar"

    if os.path.exists(year_tar_file):
        print(year_tar_file, "already exists.")

    else:
        print("copying file from institute server:", remote_tar_file, flush=True)
        # copy with scp the respective .tar file from
        ssh = SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(hostname='ostro.meteo.uni-koeln.de',
                    username='pbigalke',
                    password='nlePwVg,4amPinu',
                    look_for_keys=False)

        # SCPCLient takes a paramiko transport as its only argument
        scp = SCPClient(ssh.get_transport())
        scp.get(remote_tar_file, year_tar_file)
        scp.close()

     # define year path
    year_path = year_tar_file.replace(".tar", "")

    # unpack year folder if not already exists
    if not os.path.exists(year_path):
        print("unpacking", year_tar_file, flush=True)
        # Extract all subfolders to the specified directory
        with tarfile.open(year_tar_file, "r") as tar:
            tar.extractall(path=year_path)
    else:
        print("already unpacked", year_tar_file, flush=True)

    year_total = 0
    # loop over months
    for month in months:
        
        # get all daily files
        day_files = sorted(glob(f"{year_path}/{year}/{month:02d}/*.nc"))
    
        # loop over files and upload to bucket
        if len(day_files) > 0:
                
            for file in day_files:
                #Uploading a file to the bucket (make sure you have write access)
                object_name = f"{year}/{month:02d}/{os.path.basename(file)}"
                # Open file in binary mode and upload
                upload_file(s3, file, BUCKET_NAME, object_name=object_name)

        print("- month: ", month, " files found: ", len(day_files), flush=True)	
        year_total += len(day_files)

    print("Year: ", year, " files found: ", year_total, flush=True)	
    total += year_total

    # delete extracted year folder
    if delete_extracted:
        print(f"deleting", year_path, flush=True)
        shutil.rmtree(year_path)

    if delete_tar_after_upload:
        print("deleting", year_tar_file, flush=True)
        os.remove(year_tar_file)


print("Total files uploaded: ", total, flush=True)
t = time.time() - start_time
print("Time taken to extract and upload files: ", f"{t/3600:.2f} hours or {t/60:.2f} minutes.", flush=True)# %%


# %%
# # List the objects in our bucket to check if the files were uploaded
# response = s3.list_objects(Bucket=BUCKET_NAME)
# n = 0
# for item in response['Contents']:
#     # print(item['Key'])
#     n += 1
# print("Total files in the bucket: ", n)
# %%
