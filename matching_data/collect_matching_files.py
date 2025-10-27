import glob
import os
import numpy as np
import pandas as pd
import sys
sys.path.append("..")
import helpers.datetime_helper as hlp

def get_mwcch_files_in_study_period(mwcch_directory, detectors, years, months=None, days=None):
    
    if detectors is not None and not isinstance(detectors, list):
        detectors = [detectors]
    if years is not None and not isinstance(years, list):
        years = [years]
    if months is None:
        months = np.arange(1, 13, 1)
    else:
        if not isinstance(months, list):
            months = [months]
    if days is None:
        days = np.arange(1, 32, 1)
    else:
        if not isinstance(days, list):
            days = [days]

    mwcch_files = []

    for year in years:
        for month in months:
            for day in days:
                for f in glob.glob(f"{mwcch_directory}/{year}/{month:02}/{day:02}/*.nc"):
                    for detector in detectors:
                        if detector in f:
                            mwcch_files.append(f)

    return mwcch_files

def get_files_in_study_period(directory, years, months=None, days=None):
    
    if years is not None and not isinstance(years, (list, np.ndarray)):
        years = [years]
    if months is None:
        months = np.arange(1, 13, 1)
    else:
        if not isinstance(months, (list, np.ndarray)):
            months = [months]
    if days is None:
        days = np.arange(1, 32, 1)
    else:
        if not isinstance(days, (list, np.ndarray)):
            days = [days]

    all_files = []

    for year in years:
        for month in months:
            for day in days:
                path_day = f"{directory}/{year}/{month:02}/{day:02}"
                if os.path.exists(path_day):
                    for f in sorted(glob.glob(f"{path_day}/*.nc")):
                        all_files.append(f)

    return all_files

def get_msg_daily_files_in_study_period(directory, years, months=None, days=None):
    
    if years is not None and not isinstance(years, list):
        years = [years]
    if months is None:
        months = np.arange(1, 13, 1)
    else:
        if not isinstance(months, list):
            months = [months]
    if days is None:
        days = np.arange(1, 32, 1)
    else:
        if not isinstance(days, list):
            days = [days]

    all_files = []

    for year in years:
        for month in months:
            path_month = f"{directory}/{year}/{month:02}"
            if os.path.exists(path_month):
                for day in days:
                    for f in glob.glob(f"{path_month}/{year}{month:02}{day:02}-EXPATS-RG.nc"):
                        all_files.append(f)

    return all_files

def get_file_at_msg_timestamp(directory, timestamp, msg_res=15):

    dt = hlp.get_datetimestring_from_npdatetime(timestamp)

    # read in all files in directory that are close to timestamp
    files_to_check = glob.glob(f"{directory}/{dt[:4]}/{dt[4:6]}/{dt[6:8]}/{dt[:8]}*_E{dt[9:11]}*.nc")

    closest_files = []
    for f in files_to_check:
        start_msg = int(dt[-4:])
        end_msg = start_msg + msg_res if (int(dt[-2:])+msg_res) < 60 else start_msg + (40+msg_res)
        start_data = int(os.path.basename(f).split('_')[1][1:])
        end_data = int(os.path.basename(f).split('_')[2][1:])
        if start_msg < start_data and start_data < end_msg \
            or start_msg < end_data and end_data < end_msg:
            closest_files.append(f)

    return closest_files

def get_closest_MSG_timestamps(npdatetime, which="closest", msg_res=15):
    """
    Get the closest or neighboring MSG timestamps for a given datetime or list of datetimes.
    Args:
        npdatetime (np.datetime64 or list of np.datetime64): The datetime(s).
        which (str, optional): Specifies which timestamp to return. Options are:
                               - "closest" (default): Rounds to the nearest MSG timestamp.
                               - "previous": Rounds down to the previous MSG timestamp.
                               - "following": Rounds up to the following MSG timestamp.
                               - "both": Returns both the previous and following MSG timestamp.
        msg_res (int, optional): The MSG resolution in minutes. Defaults to 15 minutes.
    Returns:
        np.datetime64 or list of np.datetime64: The rounded datetime(s). If the input was a single datetime,
                                                a single rounded datetime is returned. If the input was a list
                                                of datetimes, a list of rounded datetimes is returned.
    """
    def process_timestamp(npdatetime):
        if which == "previous" or which == "both":
            # Round down to the nearest 15-minute interval
            rounded_down = pd.Timestamp(npdatetime).floor(f'{msg_res}T')
            return rounded_down.to_datetime64()

        elif which == "following" or which == "both":
            # Round up to the nearest 15-minute interval
            rounded_up = pd.Timestamp(npdatetime).ceil(f'{msg_res}T')
            return rounded_up.to_datetime64()
        
        else:
            # Find closest MSG timestamp
            round_closest = pd.Timestamp(npdatetime).round(f'{msg_res}min')
            return round_closest.to_datetime64()

    if isinstance(npdatetime, list):
        return [process_timestamp(dt) for dt in npdatetime]
    else:
        return process_timestamp(npdatetime)

