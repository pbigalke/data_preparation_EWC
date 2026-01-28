# Description: This script creates lists of MWCCH hail probability data files that meet certain criteria. 
#              For one thing you can extract files for a certain study period (years, months, days).
#              And for another thing you can set a specific area coverage threshold, so then the list contains 
#              only files of those overpasses covering at least that percentage of the total EXPATS domain area.
#              It allows for efficient retrieval of files meeting certain criteria for further analysis.  
# %%
import numpy as np
import os
import sys
sys.path.append("..")
import readers.read_processed_MWCCH as mwcch
import data_buckets_IO.data_buckets_read_and_write as bucket
from readers.read_processed_MWCCH import get_bucket_name
# get current directory
dir_name = os.path.dirname(__file__)

# %%
# main function to read a list of MWCCH files for given study settings
def read_mwcch_files_for_study_settings(years, months, days=np.arange(1, 32, 1), area_threshold=30):
    """
    Read MWCCH files for given study settings from pre-created file lists.
    If the file list does not exist, a warning is printed and the file list is first created.
    Args:
        years (list or int): List of years or single year
        months (list or int): List of months or single month
        days (list or int): List of days or single day
        area_threshold (int): Threshold for area coverage percentage
    """
    # get filename for given study settings
    filename = get_list_filename(years, months, days, area_threshold)

    # check if file exists
    if not os.path.exists(filename):
        print(f"\nFile {filename} \ndoes not exist yet. " + \
              "The respective file list is now being created for these study settings.\n")
        # if it does not exist, create it
        create_file_list_per_area_thresholds(get_bucket_name(), years, months, days, area_thresholds=[area_threshold])
    
    # read file lines
    with open(filename, 'r') as file:
        lines = file.readlines()[1:]  # Read all lines and skip the first one
    return [line.strip() for line in lines]  # Strip newline characters

# %%
# function to create file lists for given study settings and area thresholds
def get_list_filename(years, months, days, area_threshold):
    """
    Returns the filename for the list of files for given study settings

    : param years (list or int): List of years or single year.
    : param months (list or int): List of months or single month.
    : param days (list or int): List of days or single day.
    : param area_threshold (int): Threshold for area coverage percentage.

    Returns:
        Filename for the list of files (str)
    """
    # make sure years and months are lists
    if not isinstance(years, list) and not isinstance(years, np.ndarray):
        years = [years]
    if not isinstance(months, list) and not isinstance(years, np.ndarray):
        months = [months]
    if not isinstance(days, list) and not isinstance(days, np.ndarray):
        days = [days]

    # define output path
    path = f"{dir_name}/mwcch_file_lists"
    
    # check if path exists
    if not os.path.exists(path):
        os.makedirs(path)

    # create output file name
    year_range = f"{years[0]}" if len(years) == 1 else f"{years[0]}-{years[-1]}"
    month_range = f"{months[0]}" if len(months) == 1 else f"{months[0]}-{months[-1]}"
    day_range = f"{days[0]}" if len(days) == 1 else f"{days[0]}-{days[-1]}"
    output_file_name = f"{path}/files_{year_range}_{month_range}_{day_range}_areathresh{area_threshold}.txt"

    return output_file_name

def create_file_list_per_area_thresholds(years, months, days, area_thresholds=[10, 20, 30, 40, 50, 60]):
    """
    collect all files within given study period and then for each area threshold, save those with area larger 
    than the threshold to a respective txt file.

    Args:
        years (list or int): List of years or single year
        months (list or int): List of months or single month
        days (list or int): List of days or single day
        area_thresholds (list): List of area coverage thresholds (in percentage)    
    """
    # create txt file for each threshold
    for t in area_thresholds:
        with open(get_list_filename(years, months, days, t), "w") as f:
            # add header to file
            f.write(f"Files with area larger than {t}%\n")

    # get all files within study period
    s3 = bucket.Initialize_s3_client()
    mwcch_files = bucket.list_objects_within_study_period(s3, get_bucket_name(), years, months, days)

    # loop over files
    for f, file in enumerate(mwcch_files):

        # read from bucket
        mwcch_data = mwcch.read_mwcch_from_bucket(file, s3, variables=["hail_class"]).hail_class.values

        # get covered area percentage
        area_perc = mwcch.area_percentage_covered_by_overpass(mwcch_data)

        # check if area is larger than threshold
        for t in area_thresholds:
            if area_perc >= t:
                # write to respective file
                with open(get_list_filename(years, months, days, t), "a") as fl:
                    fl.write(f"{file}\n")

        if f % 1000 == 0 and f > 0:
            print(f"{f}/{len(mwcch_files)} files viewed..", flush=True)


# %%
