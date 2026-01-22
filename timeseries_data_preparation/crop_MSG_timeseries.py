# This script crops out random timeseries samples from MSG data stored in an S3 bucket
# The crops are taken from 4 quadrants of the full MSG domain

# %%
import io
import xarray as xr
import random
import time
import numpy as np
from scipy.ndimage import binary_closing
import os
import sys
sys.path.append('..')
from data_buckets_IO.data_buckets_read_and_write import read_file, Initialize_s3_client
from data_buckets_IO.bucket_information import get_bucket_prefix

# %%
# methods used to select crops and save them 
def search_timewindow_without_nan(ds_day, start_time, n_frames, verbose=False):
    """Search for a timeseries window without NaN values in the dataset

    :param ds_day: Dataset for the current day
    :param start_time: Start time of the timeseries window
    :param n_frames: Number of frames in the timeseries window
        
    :return: ds_timeseries: Dataset for the timeseries window
             start_time_next: Start time of the next timeseries window
    """
    # get current end time
    end_time = start_time + n_frames

    # flag to indicate if still searching for timeseries window without NaNs
    searching_timeseries_window = True
    # loop until a timeseries window without NaNs is found
    while searching_timeseries_window:
        
        # slice small timeseries from dataset
        ds_timeseries = ds_day.isel(time=slice(start_time, end_time))
        if verbose:
            print("\n", ds_timeseries.time.values[0], ds_timeseries.time.values[-1], flush=verbose)
        
        # check at each timestamp if all data is NaN, i.e. MSG timestamp is missing
        is_all_nan = ds_timeseries[CHANNEL].isnull().all(dim=['lat', 'lon'])
        if verbose:
            print("missing timestamps: ", is_all_nan.values, flush=verbose)

        # move on to after missing timestamp
        if is_all_nan.any():
            # moving to next available timestamp
            if verbose:
                print(f"Skipping chunk {ds_timeseries.time.values[0]} due to a missing timestamp.", flush=verbose)
            ind_nan_last = np.where(is_all_nan.values)[0][-1]

            # shift start and end time to next available timestamp
            start_time += ind_nan_last + 1
            end_time = start_time + n_frames

            # check if start time is still within the dataset
            if start_time >= len(ds_day.time.values):
                # if not, end this loop
                searching_timeseries_window = False
                start_time_next = None
                ds_timeseries = None
                break

            # if start and end within daytime - continue to the next iteration of the loop   
            continue
        
        # end the loop if all timestamps are available
        searching_timeseries_window = False

        # set the start and end time for the next timeseries
        start_time_next = end_time

    return ds_timeseries, start_time_next

def crop_from_quadrant(ds_timeseries, i, j, cropsize, maxoverlap, max_cropping_attempts):
    """Crop a random subcrop from the dataset within one of the quadrants denoted by i and j
    :param ds_timeseries: xarray dataset of the timeseries window
    :param i: Index of the quadrant row (0 or 1)
    :param j: Index of the quadrant column (0 or 1)
    :param cropsize: Size of the crop
    :param maxoverlap: Maximum allowed overlap of the crops among each other, this is affecting the range of the random selection
    :param max_cropping_attempts: Maximum number of attempts to find a crop without NaN values
    :return: crop_timeseries: Cropped dataset
             n_attempts: Number of attempts to crop the dataset
    """
    # get length of lat and lon dimensions
    len_lon, len_lat = ds_timeseries.sizes['lon'], ds_timeseries.sizes['lat']
    
    # counter for cropping attempts
    n_attempts = 0
    # flag to indicate if a crop was generated
    crop_generated = False
    crop_timeseries = None

    # loop until either a crop is generated or maximum number of attempts is reached
    # (in case of many NaNs in the data at some point - after max_cropping_attempts - the creation of crops is aborted)
    while n_attempts < max_cropping_attempts and not crop_generated:

        # get range of lon and lat in which the crops are randomly selected
        # depending on i and j indicating position of quadrant within domain
        min_idx_lon = 0 if i==0 else int(len_lon/2 - maxoverlap/2*cropsize)
        max_idx_lon = int(len_lon/2 - cropsize + maxoverlap/2*cropsize) if i==0 else int(len_lon - cropsize)
        min_idx_lat = 0 if j==0 else int(len_lat/2 - maxoverlap/2*cropsize)
        max_idx_lat = int(len_lat/2 - cropsize + maxoverlap/2*cropsize) if j==0 else int(len_lat - cropsize)

        # randomly select origin of crop in given lon and lat ranges (exclusive of higher threshold)
        idx_lon = np.random.randint(min_idx_lon, max_idx_lon)
        idx_lat = np.random.randint(min_idx_lat, max_idx_lat)

        # crop timeseries by selected lat and lon
        crop_timeseries = ds_timeseries.isel(lon=slice(idx_lon, idx_lon+cropsize), lat=slice(idx_lat, idx_lat+cropsize))

        # check if timeseries contains any NaN values
        nan_exist = crop_timeseries[CHANNEL].isnull().any()
        if nan_exist:
            n_attempts += 1
            continue
        else:
            n_attempts += 1
            crop_generated = True

    return crop_timeseries, n_attempts

def apply_closing_on_cloud_mask(cloud_mask):
    """Apply binary closing on the cloud mask to fill small holes
    :param cloud_mask: xarray dataset of the cloud mask
    :return: cloud_mask: xarray dataset of the cloud mask with holes filled
    """
    # loop over timestamps and apply the closing algorithm of the 'holes' in the cloud mask
    for t in range(len(cloud_mask.time.values)):
        # get the cloud mask for the current timestamp
        cloud_mask_ts = cloud_mask.isel(time=t)

        # apply binary closing with a 3x3 structuring element
        structure = np.ones((3, 3), dtype=np.uint8)

        # apply the binary closing to the cloud mask
        # cloud_mask_closed[t] = binary_closing(cloud_mask_ts, structure=structure)
        cloud_mask[t] = binary_closing(cloud_mask_ts, structure=structure)

    return cloud_mask

def add_parameters_with_applied_closed_cm(crop_timeseries, vmax=300):
    """Apply the closed cloud mask to the IR_108 channel and save it in the dataset as new variable
    :param crop_timeseries: xarray dataset of the cropped timeseries
    :param vmax: Maximum value for the IR_108 channel
    :return: crop_timeseries: xarray dataset of the cropped timeseries including applied closed cloud mask
    """
    # get cloud mask from timeseries data and apply binary closing
    cloud_mask = apply_closing_on_cloud_mask(crop_timeseries['cma'])

    # apply the cloud mask to the 10.8 channel of the timeseries data
    # set the values outside the cloud mask to vmax
    IR_108_cm = crop_timeseries.IR_108.where(cloud_mask == 1, vmax)

    # save the masked data in the original dataset as additional variable
    crop_timeseries["IR_108_cm"] = IR_108_cm
                                      
    return crop_timeseries

def crop_and_save_from_all_quadrants(ds_timeseries, cropsize, max_spatial_overlap, out_path, out_basename, max_cropping_attempts=10, verbose=False):
    """Crop out random samples from all quadrants and save them as netcdf

    :param ds_timeseries: xarray dataset of the timeseries window
    :param cropsize: Size of the crops
    :param max_spatial_overlap: Maximum allowed overlap of the crops among each other as fraction of the cropsize
    :param out_path: Path to save the crops
    :param out_basename: Basename for the output files
    :param max_cropping_attempts: Maximum number of attempts to find a crop without NaN values
    :param verbose: If True, print the filename of each crop
    :return: None
    """
    # loop over 4 quadrants of the whole domain
    for i in range(2):
        for j in range(2):
            # crop out random sample with given size from quadrant
            crop_timeseries, n_attempts = crop_from_quadrant(ds_timeseries, i, j, cropsize, max_spatial_overlap, max_cropping_attempts)

            if crop_timeseries is None:
                continue
            
            # apply cm
            crop_timeseries_cm = add_parameters_with_applied_closed_cm(crop_timeseries)

            if out_path is None or out_basename is None:
                # outpath or basename not given - do not save crops and send warning
                print("WARNING: No output path or basename given - not saving crops.", flush=True)

            else:
                # get the date and time of the first timestamp in the timeseries
                date_str, time_str = str(crop_timeseries_cm.time.values[0]).split('T')
                year, month, day = date_str.split('-')

                # output_folder for this day
                output_folder = f"{out_path}/{year}/{month}/{day}"
                os.makedirs(output_folder, exist_ok=True)
                
                # generate file name
                filepath_to_save = f"{output_folder}/{out_basename}_{year}-{month}-{day}_{time_str[:2]}{time_str[3:5]}_crop{2*i+j}.nc"

                # save crop to a netcdf file
                crop_timeseries_cm.to_netcdf(filepath_to_save, mode='w')
                
                if verbose:
                    print(f"saved after {n_attempts} attempts to: ", filepath_to_save, flush=verbose)

# %%
# main method to construct timeseries dataset
def construct_timeseries_dataset(bucket_name, years, months, days, 
                                 n_frames=8, max_daily_offset=None, 
                                 cropsize=100, max_spatial_overlap=0.25, max_cropping_attempts=10, 
                                 out_path=None, out_basename=None, verbose=False):
    """
    Construct timeseries dataset by cropping out random samples
    (4 per timestamp, one from each quadrant) and saving them as netcdf files.

    :param bucket_name: Name of the S3 bucket
    :param years: list of years to process
    :param months: list of months to process
    :param days: list of days to process
    :param n_frames: Number of frames in each timeseries
    :param max_daily_offset: Maximum daily random offset for the start of the timeseries on that day as a fraction of n_frames
    :param cropsize: Size of the crops
    :param max_spatial_overlap: Maximum spatial overlap between crops within the same timestamps
    :param max_cropping_attempts: Maximum number of attempts to find a crop without NaN values
    :param out_path: Path to save the crops
    :param out_basename: Basename for the output files
    :param verbose: If True, print detailed information
    """
    # make sure input is in correct format
    if not isinstance(years, list): years = list(years)
    if not isinstance(months, list): months = list(months)
    if not isinstance(days, list): days = list(days)

    # get start time of this script
    start_time_script = time.time()
    # count days to estimate later runtime per day
    count_days = 0
    # object to store trailing timeseries of each day to process with next day
    from_previous_day = None

    # loop over years
    for year in years:
        print(f"\nProcessing year {year}...", flush=True)
        # loop over months and days
        for month in months:
            print(f"\nProcessing month {month}...", flush=True)
            # loop over days
            for day in days:

                # get filename of this day
                file = f"{get_bucket_prefix(bucket_name, year, month, day)}.nc"
                print(file, flush=True)

                # read in file from bucket if exists
                my_obj = read_file(s3, file, bucket_name)
                if my_obj is not None:
                    
                    # count days to estimate later runtime per day
                    count_days += 1

                    # open dataset
                    with xr.open_dataset(io.BytesIO(my_obj)) as ds_day:

                        if from_previous_day is not None:
                            # add the trailing timeseries of the previous day to the current day data and slice timeseries
                            ds_day = xr.concat([from_previous_day, ds_day], dim='time')

                        # no trailing data of previous day 
                        # -> if max_daily_offset is given generate random offset for first timeseries of the day to increase variability
                        start_time = 0 if max_daily_offset is None else random.randint(0, round(max_daily_offset*n_frames)+1)

                        # loop over until the end of the day
                        while start_time < len(ds_day.time.values):
                            
                            # find timeseries window without NaN values and next start time
                            ds_timeseries, start_time = search_timewindow_without_nan(ds_day, start_time, n_frames)

                            # check if current timeseries has expected length or if it is an incomplete last timeseries of the day
                            if ds_timeseries is None:
                                # if there is no valid timeseries left in the day - break the loop and move on to next day
                                if verbose:
                                    print(f"No trailing timeseries of current day due to missing timestamps - move on to next day.", flush=verbose)
                                from_previous_day = None
                                break

                            elif len(ds_timeseries.time.values) < n_frames or ds_timeseries is None:
                                # if the current timeseries is incomplete, then this is the last of the current day 
                                # -> save it and process it with the next day
                                if verbose:
                                    print(f"The last timeseries of the day is not complete - process with next day.", flush=verbose)
                                from_previous_day = ds_timeseries
                                break

                            else:
                                #if timeseries has expected length:
                                # crop out random samples from all quadrants given size and save them as netcdf
                                crop_and_save_from_all_quadrants(ds_timeseries, cropsize, max_spatial_overlap, out_path, out_basename,
                                                                max_cropping_attempts=max_cropping_attempts, verbose=verbose)
                                if verbose:
                                    print("Processing timeseries from ", ds_timeseries.time.values[0], " to ", ds_timeseries.time.values[-1], flush=True)

        # print progress
        print("----------------------------------------------", flush=True)
        temp_runtime = time.time() - start_time_script
        print(f"{count_days} days processed: {temp_runtime/count_days:.2f} seconds or {temp_runtime/count_days/60:.2f} minutes per day", flush=True)
        print(f"total runtime until now: {temp_runtime/60:.2f} minutes or {temp_runtime/60/60:.2f} hours", flush=True)

    # runnning time of the script in minutes
    runtime = time.time() - start_time_script
    print()
    print(f"Total runtime: {runtime/60:.2f} minutes or {runtime/60/60:.2f} hours", flush=True)
    print(f"Runtime per day: {runtime/count_days:.2f} seconds or {runtime/count_days/60:.2f} minutes", flush=True)

# %%
if __name__ == "__main__":

    # Initialize the S3 client (bucket)
    print('Initializing S3 client for accessing Data Bucket...')
    s3 = Initialize_s3_client()

    # name of the S3 bucket with MSG data
    S3_BUCKET_NAME = 'expats-msg-training' 

    #Directory with the data to upload
    years = [2015]  # np.arange(2013, 2024, 1)
    months = [4]  # np.arange(4, 10, 1)
    days = [27, 28, 29]  # np.arange(1, 32, 1) #[9, 10, 11]

    # parameters for temporal cropping
    n_frames = 8 #, 10, 12, 14, 16]
    max_daily_offset = 0.25  # one can set a random offset in percent of n_frames at the beginning of the day to introduce a randomness in the timeseries starting times

    # parameters for random spatial cropping
    cropsize = 100
    max_spatial_overlap = 0.25 # maximum allowed overlap of the crops among each other as fraction of the cropsize
    max_cropping_attempts = 10 # maximum number of attempts to find a crop without NaN values

    # where and how to save the crops
    out_path = "output/data/timeseries_crops"
    out_basename = "MSG_timeseries"

    # prints progress information if true
    verbose = True

    # run preparation of timeseries dataset
    construct_timeseries_dataset(S3_BUCKET_NAME, years, months, days, 
                                 n_frames, max_daily_offset, 
                                 cropsize, max_spatial_overlap, max_cropping_attempts, 
                                 out_path=out_path, out_basename=out_basename, verbose=verbose)

# %%
