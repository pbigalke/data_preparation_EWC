# %%
import numpy as np
import os
import sys
sys.path.append("..")
import readers.read_processed_MWCC_H as mwcch_read
import matching_data.collect_matching_files as match

# %%
def get_list_filename(mwcch_path, years, months, area_threshold):
    # make sure years and months are lists
    if not isinstance(years, list) and not isinstance(years, np.ndarray):
        years = [years]
    if not isinstance(months, list) and not isinstance(years, np.ndarray):
        months = [months]
    
    # define output path
    path = f"{mwcch_path}/file_lists_studies/"
    
    # check if path exists
    if not os.path.exists(path):
        os.makedirs(path)

    # create output file name
    year_range = f"{years[0]}" if len(years) == 1 else f"{years[0]}-{years[-1]}"
    month_range = f"{months[0]}" if len(months) == 1 else f"{months[0]}-{months[-1]}"
    output_file_name = f"{path}/files_{year_range}_{month_range}_areathresh{area_threshold}.txt"
    
    return output_file_name

def create_file_list_per_area_thresholds(mwcch_path, years, months, area_thresholds=[10, 20, 30, 40, 50, 60]):
    """collect all files with overpass area larger than area_threshold and save to txt files

    Parameters
    ----------
    area_threshold : list of int, optional
        _description_, by default 30
    """
    # create txt file for each threshold
    for t in area_thresholds:
        with open(get_list_filename(mwcch_path, years, months, t), "w") as f:
            # add header to file
            f.write(f"Files with area larger than {t}%\n")

    count = 0
    # loop over subfolders
    for year in years:
        for month in months:
            # collect all mwcc-h files in this month
            mwcch_files = match.get_files_in_study_period(mwcch_path, year, months=month)

            # loop over files
            for file in mwcch_files:
                # read in dataset
                mwcch_data = mwcch_read.read(file, variables=["hail_class"]).hail_class.values

                # get covered area percentage
                area_perc = mwcch_read.get_area_percentage_covered_by_overpass(mwcch_data)

                # check if area is larger than threshold
                for t in area_thresholds:
                    if area_perc >= t:
                        # write to respective file
                        with open(get_list_filename(mwcch_path, years, months, t), "a") as f:
                            f.write(f"{file}\n")

                if count % 1000 == 0:
                    print(f"{count}", flush=True)
                count += 1

def read_mwcch_files_for_study_settings(mwcch_path, years, months, area_threshold):

    # if area threshold is 0, return all files
    if area_threshold == 0:
        return match.get_files_in_study_period(mwcch_path, years, months=months)
    
    filename = get_list_filename(mwcch_path, years, months, area_threshold)
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
    years = np.arange(2006, 2024, 1)
    months = np.arange(4, 10, 1)
    area_threshold = 30
    files = read_mwcch_files_for_study_settings(mwcch_bucket, years, months, area_threshold)
    print(len(files))
    for f in files[:10]:
        print(f)

# %%
