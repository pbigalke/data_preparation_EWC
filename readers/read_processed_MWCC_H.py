
# %%
import xarray as xr
import numpy as np
import re
import os
import sys
sys.path.append("..")
import matching_data.collect_matching_files as clct
import helpers.datetime_helper as hlp

MWCCH_PATH = "/data/sat/products/PMW_sats/MWCCH_hail_probability/netcdf"
MWCCH_MSGGRID_PATH = "/data/sat/products/PMW_sats/MWCCH_hail_probability/netcdf_MSG_grid"
ALL_VARS = ["datetime", "cloud_type", "TB", "POH", "hail_class"]

# %%
def read(file_path, variables=ALL_VARS):
    """ read processed MWCC-H output containing probability of hail
    """
    if not isinstance(variables, list):
        variables = [variables]

    # get variables to drop
    droplist = [var for var in ALL_VARS if var not in variables]

    # read in dataset and drop variables that are not needed
    with xr.open_dataset(file_path, engine="h5netcdf", drop_variables=droplist) as dataset:
        return dataset

# %%
hail_class_dict = {
    0: "no_hail", 
    1: "hail_potential", 
    2: "hail_initiation_graupel", 
    3: "large_hail", 
    4: "super_hail", 
}

def get_hail_classes(type="number"):
    if type == "number":
        return list(hail_class_dict.keys())
    elif type == "name":
        return list(hail_class_dict.values())
    
def convert_POH_to_hail_class(poh, type="number"):
    # define hail classes, the entry np.NaN is assigned to poh=NaN
    if type == "name":
        hail_classes = ["no_hail", 
                        "hail_potential", 
                        "hail_initiation_graupel", 
                        "large_hail", 
                        "super_hail", 
                        np.NaN]
    else:
        hail_classes = [0, 1, 2, 3, 4, np.NaN]
    
    # if only one values is given
    if isinstance(poh, float):
        poh = np.array(poh)

    # define boundaries of hail classes
    boundaries = [0, 0.2, 0.36, 0.45, 0.6, 1.01]

    # search for hail class corresponding to given poh
    idx = np.searchsorted(boundaries, poh.ravel(), side='right') - 1
    hail_classes = np.take(hail_classes, idx)

    # reshape into original shape
    hail_classes = hail_classes.reshape(poh.shape)

    return hail_classes

def convert_hail_class(hail_class_values, to="name"):
    # which direction to convert
    if to == "number":
        # Create a reverse dictionary for name to number conversion
        reverse_hail_class_dict = {v: k for k, v in hail_class_dict.items()}

        # Define a vectorized function for conversion
        vectorized_conversion = np.vectorize(lambda x: reverse_hail_class_dict[x])
        hail_class_values = vectorized_conversion(hail_class_values)
    
    elif to == "name":
        # Define a vectorized function for conversion
        vectorized_conversion = np.vectorize(lambda x: hail_class_dict[x])
        hail_class_values = vectorized_conversion(hail_class_values)
    
    return hail_class_values

# get the maximum hail class in the hail class array
def max_hail_class(hail_class_values, min_pixel=1):
    for hail in get_hail_classes(type="number")[::-1]:
        if np.count_nonzero(hail_class_values == hail) >= min_pixel:
            return hail
    return None

# calculate area percentage covered by overpass from probability of hail values
def area_percentage_covered_by_overpass(poh_or_hail_class):
    # get total number of pixels
    N_pixel = poh_or_hail_class.shape[0] * poh_or_hail_class.shape[1]
    # get number of nan entries:
    N_nans = np.sum(np.isnan(poh_or_hail_class))
    # calculate area percentage covered by overpass
    area_perc = round((N_pixel-N_nans) / N_pixel * 100)

    return area_perc

# %%
# functions to extract information from file path
def get_y_m_d_from_mwcch_filepath(file_path):
    # get date string
    date = get_datestring_from_mwcch_filepath(file_path)

    # extract date from filename
    year = int(date[:4])
    month = int(date[4:6])
    day = int(date[6:])

    return year, month, day

def get_scan_datetime_from_mwcch_filepath(file_path, which="both"):
    
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
    # Define the regular expression pattern for date
    date_pattern = r'(\d{8})'

    # Search for the patterns in the filename
    date_match = re.search(date_pattern, file_path)

    # Extract the times if the patterns are found
    if date_match:
        return date_match.group(1)
    else:
        raise ValueError("Date pattern not found in filename")

def get_satellite(file_path=None):
    satellites = ['meto01', 'meto02', 'meto03', 'noaa15', 'noaa16', 'noaa17', 'noaa18', 'noaa19', 
                  'n20', 'n21', 'npp', 'f16', 'f17', 'gpm']
    if file_path is None:
        return satellites
    
    for sat in satellites:
        if sat in file_path.lower():
            return sat
    return None

def get_detector_from_mwcch_filepath(file_path):
    detectors = ['ATMS', 'MHS', 'SSMIS', 'GMI']
    for det in detectors:
        if det.lower() in file_path.lower():
            return det
    return None

def generate_mwcch_filepath(path, start_dt, end_dt, detector, satellite, suffix=""):
    # get date string from start datetime
    date_string = hlp.get_datestring_from_npdatetime(start_dt)

    # get starting and end time within our domain
    start_time = f"S{hlp.get_timestring_from_npdatetime(start_dt)}"
    end_time = f"E{hlp.get_timestring_from_npdatetime(end_dt)}"
    
    # define netcdf file name
    date_path = f"{path}/{date_string[:4]}/{date_string[4:6]}/{date_string[6:]}"
    if not os.path.exists(date_path):
        os.makedirs(date_path)
    file_path = f"{date_path}/{date_string}_{start_time}_{end_time}_{detector}_{satellite}{suffix}.nc"
    
    return file_path

# %%
if __name__ == '__main__':
    import numpy as np
    # test on exaple file
    example_file = "mhs_METOPB_20230724-S1905-E2046_056289"
    satellite = 'METOPB'
    
    path = "/net/merisi/pbigalke/data/MWCC-H/netcdf"
    years = [2022]
    months = [6]
    days = [5]
    detectors = ["ATMS", "MHS", "SSMIS"]
    all_files = clct.get_mwcch_files_in_study_period(path, detectors, years, months, days)
    for f in all_files:
        print(f)

# %%
