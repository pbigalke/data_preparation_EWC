# %%
import xarray as xr
import os
import sys
sys.path.append('..')
import helpers.datetime_helper as hlp

MSG_PATH = "/data/sat/msg/netcdf/parallax"

CHANNELS = ["IR_016", "IR_039", "IR_087", "IR_097", "IR_108", "IR_120", "IR_134",
            "VIS006", "VIS008", "WV_062", "WV_073"]

# %%
def read(msg_file, channels=None):
    """Read MSG file and return xarray dataset

    Args:
        msg_file (pathlike): path to MSG file
        channels (list(str), optional): list of channel names that should be read in. Defaults to None.

    Returns:
        xr.dataset: xarray dataset containing MSG data
    """
    drop = [ch for ch in CHANNELS if ch not in channels] if channels is not None else None
    with xr.open_dataset(msg_file, drop_variables=drop) as dataset:
        return dataset

def get_y_m_d_from_filepath(msg_file):
    """Extract year, month and day from MSG file path

    Args:
        msg_file (pathlike): path to MSG file

    Returns:
        str, str, str: year, month, day
    """
    name = os.path.basename(msg_file)
    date = name.split('_')[0]
    return date[:4], date[4:6], date[6:]

def get_lon_lat():
    """Get longitude and latitude values of regular gridded MSG files given in MSG_PATH

    Returns:
        np.array(float), np.array(float): longitude and latitude values
    """
    MSG_example_file = f"{MSG_PATH}/2023/09/20230930-EXPATS-RG.nc"
    with xr.open_dataset(MSG_example_file, drop_variables=CHANNELS) as dataset:
        lon = dataset.lon.values
        lat = dataset.lat.values
    return lon, lat

def get_MSG_file_from_timestamp(msg_dt):
    """Get MSG file from timestamp

    Args:
        msg_dt (np.datetime64): MSG timestamp

    Returns:
        pathlike: corresponding MSG file
    """
    # convert to string
    dt_str = hlp.get_datestring_from_npdatetime(msg_dt)

    # get corresponding MSG file containing this timestamp
    msg_file = f"{MSG_PATH}/{dt_str[:4]}/{dt_str[4:6]}/{dt_str}-EXPATS-RG.nc" #20220615-EXPATS-RG.nc
    
    return msg_file

