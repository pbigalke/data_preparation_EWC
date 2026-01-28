# Description: Functions to read processed MWCCH files from EWC bucket and extract and calculate useful information
# %%
import xarray as xr
import numpy as np
import re
import io
import sys
sys.path.append("..")
from data_buckets_IO.data_buckets_read_and_write import read_file

# define MWCCH bucket name and all variables that are stored in the files
MWCCH_BUCKET_NAME = "mwcch-hail-regrid-msg"
ALL_VARS = ["datetime", "cloud_type", "TB", "POH", "hail_class"]

# %%
# main function to read MWCCH files from EWC bucket
def read_mwcch_from_bucket(file_name, s3, variables=ALL_VARS):
    """
    Read processed MWCC-H file from EWC bucket containing probability of hail and hail classes, etc.
    
    :param file_name: Full path of the file in the S3 bucket.
    :param s3: S3 client or resource object used to access the bucket. Initialized outside of this function (see data_buckets_read_and_write.py).
    :param bucket_name: Name of the S3 bucket where the file is stored, default is MWCCH_BUCKET_NAME.
    :param variables: List of variables to read from the file. Default is ALL_VARS. If content not known yet, read all variables.
    :return: xarray.Dataset containing the requested variables from the MWCCH file, or None if the file could not be read.
    :rtype: xarray.Dataset | None
    """
    if not isinstance(variables, list):
        variables = [variables]

    # get variables to drop
    droplist = [var for var in ALL_VARS if var not in variables]

    # get object from bucket
    my_obj = read_file(s3, file_name, MWCCH_BUCKET_NAME)

    # if object is not None open as xarray dataset
    if my_obj is not None:
        with xr.open_dataset(io.BytesIO(my_obj), drop_variables=droplist) as ds:
            return ds

# %%
# functions to get information about MWCCH files and hail classes
def get_bucket_name():
    """
    Get the name of the MWCCH bucket.

    :return: Name of the MWCCH bucket.
    :rtype: str
    """
    return MWCCH_BUCKET_NAME

HAIL_CLASS_DICT = {
    0: "no_hail", 
    1: "hail_potential", 
    2: "hail_initiation_graupel", 
    3: "large_hail", 
    4: "super_hail", 
}
def get_hail_classes(type="number"):
    """
    Get list of hail classes either as numbers or names.

    :param type: whether to return "number" or "name" of hail classes.
    :return: List of hail classes as numbers or names.
    :rtype: list
    """
    if type == "number":
        return list(HAIL_CLASS_DICT.keys())
    elif type == "name":
        return list(HAIL_CLASS_DICT.values())

def convert_hail_class(hail_class_values, to="name"):
    """
    Convert hail class values between number and name representation.

    :param hail_class_values: Array of hail class values, from e.g. an MWCCH file.    
    :param to: Direction of conversion, either "number" or "name".
    :return: Converted hail class values.
    :rtype: np.ndarray
    """
    # which direction to convert
    if to == "number":
        # Create a reverse dictionary for name to number conversion
        reverse_hail_class_dict = {v: k for k, v in HAIL_CLASS_DICT.items()}

        # Define a vectorized function for conversion
        vectorized_conversion = np.vectorize(lambda x: reverse_hail_class_dict[x])
        hail_class_values = vectorized_conversion(hail_class_values)
    
    elif to == "name":
        # Define a vectorized function for conversion
        vectorized_conversion = np.vectorize(lambda x: HAIL_CLASS_DICT[x])
        hail_class_values = vectorized_conversion(hail_class_values)
    
    return hail_class_values

def get_max_hail_class(hail_class_values, min_pixel=1):
    """
    Get the maximum hail class present in the hail class array, 
    considering only classes with at least min_pixel occurrences.

    :param hail_class_values: Array of hail class values, from e.g. an MWCCH file.
    :param min_pixel: Minimum number of pixels required to consider this hail class.
    :return: Maximum hail class present in the array that meets the min_pixel requirement, or None if none found.
    :rtype: int | None
    """
    # loop over hail classes from largest to smallest
    for hail in get_hail_classes(type="number")[::-1]:
        if np.count_nonzero(hail_class_values == hail) >= min_pixel:
            return hail
    return None

def area_percentage_covered_by_overpass(poh_or_hail_class):
    """
    Calculate percentage of EXPATS domain area covered by overpass from probability of hail values or hail class values.

    :param poh_or_hail_class: Array of probability of hail values or hail class values.
    :return: Area percentage covered by overpass (float).
    :rtype: float
    """
    # get total number of pixels
    N_pixel = poh_or_hail_class.shape[0] * poh_or_hail_class.shape[1]
    # get number of nan entries:
    N_nans = np.sum(np.isnan(poh_or_hail_class))
    # calculate area percentage covered by overpass
    area_perc = round((N_pixel-N_nans) / N_pixel * 100)

    return area_perc

# %%
# functions to extract information from file path
def get_scan_datetime_from_mwcch_filepath(file_path, which="both"):
    """
    Extract scanning start and/or end datetime from MWCCH file path.
    
    :param file_path: Full path of the MWCCH file.
    :param which: Specify which datetime to extract: "start", "end", or "both". Default is "both".
    """
    # get date string
    date = get_datestring_from_mwcch_filepath(file_path)
    
    # get start and end times
    starttime, endtime = get_start_and_end_timestrings_from_mwcch_filepath(file_path)

    # convert to datetime and return the requested datetime
    if which == "start":
        start_datetime = np.datetime64(f'{date[:4]}-{date[4:6]}-{date[6:]}T{starttime[:2]}:{starttime[2:]}')
        return start_datetime
    elif which == "end":
        end_datetime = np.datetime64(f'{date[:4]}-{date[4:6]}-{date[6:]}T{endtime[:2]}:{endtime[2:]}')
        return end_datetime
    else:
        # convert to datetime
        start_datetime = np.datetime64(f'{date[:4]}-{date[4:6]}-{date[6:]}T{starttime[:2]}:{starttime[2:]}')
        end_datetime = np.datetime64(f'{date[:4]}-{date[4:6]}-{date[6:]}T{endtime[:2]}:{endtime[2:]}')
        return start_datetime, end_datetime

def get_start_and_end_timestrings_from_mwcch_filepath(file_path):
    """
    Extract scanning start and end timestrings from MWCCH file path.
        
    :param file_path: Full path of the MWCCH file.
    :return: Tuple containing start and end timestrings in the format 'HHMM'.
    :rtype: tuple of str
    """
    # Define the regular expression patterns for start and end times
    start_pattern = r'_S(\d{4})_'
    end_pattern = r'_E(\d{4})_'

    # Search for the patterns in the filename
    start_match = re.search(start_pattern, file_path)
    end_match = re.search(end_pattern, file_path)

    # Extract the times if the patterns are found
    if start_match and end_match:
        start_time = start_match.group(1)
        end_time = end_match.group(1)
        return start_time, end_time
    else:
        raise ValueError("Start or end time pattern not found in filename")
    
def get_datestring_from_mwcch_filepath(file_path):
    """
    Extract date string from MWCCH file path.

    :param file_path: Full path of the MWCCH file.
    :return: Date string in the format 'YYYYMMDD'.
    :rtype: str
    """
    # Define the regular expression pattern for date
    date_pattern = r'(\d{8})'

    # Search for the patterns in the filename
    date_match = re.search(date_pattern, file_path)

    # Extract the times if the patterns are found
    if date_match:
        return date_match.group(1)
    else:
        raise ValueError("Date pattern not found in filename")


# %%
