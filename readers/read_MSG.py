# Description: Functions to read MSG files from EXPATS bucket and extract useful information
# %%
import xarray as xr
import io
import sys
sys.path.append('..')
import helpers.datetime_helper as hlp
from data_buckets_IO.data_buckets_read_and_write import read_file
from data_buckets_IO.bucket_information import get_bucket_prefix

MSG_BUCKET = "expats-msg-training"
CHANNELS = ["IR_016", "IR_039", "IR_087", "IR_097", "IR_108", "IR_120", "IR_134",
            "VIS006", "VIS008", "WV_062", "WV_073"]

# %%
# main function to read MSG files from EXPATS bucket
def read_MSG_from_bucket(msg_file, s3, channels=CHANNELS):
    """
    Read MSG file from bucket and return xarray dataset

    :param msg_file (pathlike): full MSG file name
    :param channels (list(str), optional): list of channel names that should be read in. Defaults to CHANNELS.

    :return xr.Dataset: xarray dataset containing MSG data
    """
    # get drop list
    if isinstance(channels, str):
        channels = [channels]
    droplist = [ch for ch in CHANNELS if ch not in channels] if channels is not None else None

    # get object from bucket
    my_obj = read_file(s3, msg_file, MSG_BUCKET)

    # if object is not None open as xarray dataset
    if my_obj is not None:
        with xr.open_dataset(io.BytesIO(my_obj), drop_variables=droplist) as ds:
            return ds

# %%
# function to get MSG file from timestamp
def get_MSG_file_from_timestamp(msg_dt):
    """
    Get MSG file from timestamp

    :param msg_dt (np.datetime64): MSG timestamp
    :return pathlike: corresponding MSG file
    """
    # convert to string
    dt_str = hlp.get_datestring_from_npdatetime(msg_dt)

    # extract year, month, day
    year, month, day = int(dt_str[:4]), int(dt_str[4:6]), int(dt_str[6:8])

    # get bucket prefix
    prefix = get_bucket_prefix(MSG_BUCKET, year, month, day)

    # get corresponding MSG file containing this timestamp
    msg_file = f"{prefix}.nc" 
    
    return msg_file

