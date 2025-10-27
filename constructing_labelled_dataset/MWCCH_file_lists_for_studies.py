# %%
import numpy as np
import os
import sys
sys.path.append("..")
import readers.read_processed_MWCC_H as mwcch_read
import matching_data.collect_matching_files as match
from data_buckets_IO.data_buckets_read_and_write import Initialize_s3_client, list_objects_within_study_period, download_file
# get current directory
dir_name = os.path.dirname(__file__)

# %%
def get_list_filename(years, months, days, area_threshold):
    # make sure years and months are lists
    if not isinstance(years, list) and not isinstance(years, np.ndarray):
        years = [years]
    if not isinstance(months, list) and not isinstance(years, np.ndarray):
        months = [months]
    if not isinstance(days, list) and not isinstance(days, np.ndarray):
        days = [days]

    # define output path
    path = f"{dir_name}/mwcch_file_lists/"
    
    # check if path exists
    if not os.path.exists(path):
        os.makedirs(path)

    # create output file name
    year_range = f"{years[0]}" if len(years) == 1 else f"{years[0]}-{years[-1]}"
    month_range = f"{months[0]}" if len(months) == 1 else f"{months[0]}-{months[-1]}"
    day_range = f"{days[0]}" if len(days) == 1 else f"{days[0]}-{days[-1]}"
    output_file_name = f"{path}/files_{year_range}_{month_range}_{day_range}_areathresh{area_threshold}.txt"

    return output_file_name

def create_file_list_per_area_thresholds(mwcch_bucket, years, months, days, area_thresholds=[10, 20, 30, 40, 50, 60]):
    """collect all files with overpass area larger than area_threshold and save to txt files

    Parameters
    ----------
    area_threshold : list of int, optional
        _description_, by default 30
    """
    # create txt file for each threshold
    for t in area_thresholds:
        with open(get_list_filename(years, months, days, t), "w") as f:
            # add header to file
            f.write(f"Files with area larger than {t}%\n")

    # get all files within study period
    s3 = Initialize_s3_client()
    mwcch_files = list_objects_within_study_period(s3, mwcch_bucket, years, months, days)

    # create temp local path
    local_path = f"{dir_name}/temp_mwcch_files"
    if not os.path.exists(local_path):
        os.makedirs(local_path)
    local_tmp_file = f"{local_path}/temp_file.nc"

    # loop over files
    for f, file in enumerate(mwcch_files):
        # download from bucket
        download_file(s3, file, mwcch_bucket, local_tmp_file)
        
        # open as dataset
        mwcch_data = mwcch_read.read(local_tmp_file, variables=["hail_class"]).hail_class.values

        # get covered area percentage
        area_perc = mwcch_read.area_percentage_covered_by_overpass(mwcch_data)

        # check if area is larger than threshold
        for t in area_thresholds:
            if area_perc >= t:
                # write to respective file
                with open(get_list_filename(years, months, days, t), "a") as fl:
                    fl.write(f"{file}\n")

        if f % 1000 == 0:
            print(f"{f}", flush=True)

    # delete temp file and folder
    os.remove(local_tmp_file)
    os.rmdir(local_path)


def read_mwcch_files_for_study_settings(mwcch_bucket, years, months, days, area_threshold):

    # if area threshold is 0, return all files
    if area_threshold == 0:
        s3 = Initialize_s3_client()
        files = list_objects_within_study_period(s3, mwcch_bucket, years, months, days)
        return files

    filename = get_list_filename(years, months, days, area_threshold)
    if not os.path.exists(filename):
        print(f"File {filename} does not exist. " + \
              "Please open the python script constructing_dataset.MWCCH_file_lists_for_studies.py " + \
              "and run method create_file_list_per_area_thresholds() for these study settings first.")
        return None

    with open(filename, 'r') as file:
        lines = file.readlines()[1:]  # Read all lines and skip the first one
    return [line.strip() for line in lines]  # Strip newline characters

# %%
if __name__ == "__main__":
    # years = np.arange(2006, 2024, 1)
    # months = np.arange(4, 10, 1)
    mwcch_bucket = "mwcch-hail-regrid-msg"
    # years = np.arange(2013, 2024, 1)
    months = np.arange(4, 10, 1)
    days = np.arange(1, 32, 1)
    area_thresholds = np.arange(0, 70, 10)

    for years in [np.arange(2013, 2024, 1), np.arange(2006, 2024, 1)]:
        create_file_list_per_area_thresholds(mwcch_bucket, years, months, days, area_thresholds=area_thresholds)

    # for area_threshold in area_thresholds[2:3]:
    #     files = read_mwcch_files_for_study_settings(mwcch_bucket, years, months, days, area_threshold)
    #     if files is None:
    #         continue
    #     print(len(files))
    #     for f in files[:10]:
    #         print(f)

# %%
