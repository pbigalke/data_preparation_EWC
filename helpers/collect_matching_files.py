# script to collect matching files based on detectors and study periods
# %%
import glob
import os
import numpy as np
import pandas as pd
import sys
sys.path.append("..")
import helpers.datetime_helper as hlp

def get_mwcch_files_in_study_period(mwcch_directory, detectors, years, months=None, days=None):
    """
    Docstring for get_mwcch_files_in_study_period
    
    :param mwcch_directory: Description
    :param detectors: Description
    :param years: Description
    :param months: Description
    :param days: Description
    """
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

def get_closest_MSG_timestamps(npdatetime, which="closest", msg_res=15):
    """
    Get the closest or neighboring MSG timestamps for a given datetime or list of datetimes.

    :param npdatetime (np.datetime64 or list of np.datetime64): The datetime(s).
    :param which (str, optional): Specifies which timestamp to return. Options are:
                               - "closest" (default): Rounds to the nearest MSG timestamp.
                               - "previous": Rounds down to the previous MSG timestamp.
                               - "following": Rounds up to the following MSG timestamp.
                               - "both": Returns both the previous and following MSG timestamp.
    :param msg_res (int, optional): The MSG resolution in minutes. Defaults to 15 minutes.
    :return: np.datetime64 or list of np.datetime64: The rounded datetime(s). If the input was a single datetime,
                                                a single rounded datetime is returned. If the input was a list
                                                of datetimes, a list of rounded datetimes is returned.
    """
    def process_timestamp(npdatetime):
        if which == "previous" or which == "both":
            # Round down to the nearest 15-minute interval
            rounded_down = pd.Timestamp(npdatetime).floor(f'{msg_res}min')
            return rounded_down.to_datetime64()

        elif which == "following" or which == "both":
            # Round up to the nearest 15-minute interval
            rounded_up = pd.Timestamp(npdatetime).ceil(f'{msg_res}min')
            return rounded_up.to_datetime64()
        
        else:
            # Find closest MSG timestamp
            round_closest = pd.Timestamp(npdatetime).round(f'{msg_res}min')
            return round_closest.to_datetime64()

    if isinstance(npdatetime, list):
        return [process_timestamp(dt) for dt in npdatetime]
    else:
        return process_timestamp(npdatetime)

