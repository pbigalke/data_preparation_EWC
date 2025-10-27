# %%
import numpy as np
import pandas as pd
import xarray as xr
import datetime
import os
import sys
sys.path.append('..')
import MWCCH_file_lists_for_studies as mwcch_list
import chunk_MWCCH_files as mwcch_chunk
import crop_over_hail_or_overpass as mwcch_crop
import readers.read_processed_MWCC_H as mwcch_read
import readers.read_MSG as msg_read
import matching_data.collect_matching_files as match
import helpers.datetime_helper as hlp


# %%
def collect_MSG_timeseries(overpass_end_time, msg_res, n_frames):

    # get MSG timestamp following the overpass end time
    last_msg_dt = match.get_closest_MSG_timestamps(overpass_end_time, 
                                                   which="following",
                                                   msg_res=msg_res)

    # extent by previous timestamps to receive MSG time series of given length ending in overpass
    time_series_dt = last_msg_dt - pd.to_timedelta(np.arange(n_frames)[::-1]*msg_res, 'm')

    # find all MSG days that this timeseries covers
    days_in_time_series = time_series_dt.normalize().unique().values

    msg_time_series = []
    # loop over MSG days
    for msg_day in days_in_time_series:

        # find corresponding msg daily file
        msg_day_file = msg_read.get_MSG_file_from_timestamp(msg_day)

        # read MSG data
        msg_data = msg_read.read(msg_day_file)

        # select only timestamps that are covered by time series
        msg_time_series.append(msg_data.where(msg_data.time.isin(time_series_dt), drop=True))

    # merge separate days into one dataset
    msg_time_series = xr.merge(msg_time_series)

    return msg_time_series

# %%
def add_attributes(msg_timeseries, cg_lon, cg_lat, cg_lon_recentered=None, cg_lat_recentered=None):
    # add global attributes about the data
    description = "MSG time series cropped over location of hail area in last frame " + \
        "detected by the PMW satellite hail probability MWCC-H."
    start_time = hlp.get_datetimestring_from_npdatetime(msg_timeseries.time.values[0])
    end_time = hlp.get_datetimestring_from_npdatetime(msg_timeseries.time.values[-1])
    n_frames = len(msg_timeseries.time.values)
    cropsize = len(msg_timeseries.lon.values)
    hail_area_lon = cg_lon
    hail_area_lat = cg_lat
    # recentered_lon = "" if cg_lon_recentered is None else cg_lon_recentered
    # recentered_lat = "" if cg_lat_recentered is None else cg_lat_recentered

    msg_timeseries = msg_timeseries.assign_attrs(description=description, 
                                                 start_time=start_time, end_time=end_time, 
                                                 n_frames=n_frames, cropsize=cropsize, 
                                                 hail_area_lon=hail_area_lon, hail_area_lat=hail_area_lat) #, 
                                                 # recentered_lon=recentered_lon, recentered_lat=recentered_lat)
    return msg_timeseries
    
def crop_MSG_timeseries_over_hail_and_save(msg_timeseries, mwcch_data, cropsize, filepath, recenter=None):

    # get extent of crop over max hail class area
    cg_lon, cg_lat, minlon, maxlon, minlat, maxlat = \
        mwcch_crop.get_crop_extent_over_maxhailarea(msg_timeseries, mwcch_data, cropsize)
    
    if recenter is not None:
        # recenter crop over highest cloud area within crop
        cg_lon_recentered, cg_lat_recentered, minlon, maxlon, minlat, maxlat = \
            mwcch_crop.recenter_crop_over_highest_clouds(msg_timeseries, [minlon, maxlon, minlat, maxlat], mode=recenter)

    # return cropped dataset
    msg_timeseries = msg_timeseries.sel(lon=slice(minlon, maxlon), lat=slice(minlat, maxlat))
    
    # add global attributes describing the data
    msg_timeseries = add_attributes(msg_timeseries, cg_lon, cg_lat, 
                                    cg_lon_recentered=None if recenter is None else cg_lon_recentered, 
                                    cg_lat_recentered=None if recenter is None else cg_lat_recentered)

    # save to given filepath
    msg_timeseries.to_netcdf(filepath)

def folder_from_study_settings(output_path, years, months, area_threshold, msg_res, n_frames, gap, cropsize, min_pix):
    folder_path = f"{output_path}/{years[0]}-{years[-1]}_{months[0]}-{months[-1]}_areathresh{area_threshold}_" + \
            f"res{msg_res}min_{n_frames}frames_gap{gap}min_cropsize{cropsize}_min{min_pix}pix"
    return folder_path

# %%
def construct_labelled_MSG_timeseries(path, years, months, area_threshold, msg_res, n_frames, gap, cropsize, min_pix):
    # mwcch_path = "/net/merisi/pbigalke/data/MWCC-H/netcdf"
    mwcch_path = mwcch_read.MWCCH_MSGGRID_PATH

    output_path = folder_from_study_settings(path, years, months, area_threshold, msg_res, n_frames, gap, cropsize, min_pix)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # ---------------------------------------------------------------- get all MWCC-H files
    # load all mwcc-h files in study period
    mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_path, years, months, area_threshold=area_threshold)
    print(f"total number of MWCC-H files in study period: {len(mwcch_files)}")

    # ---------------------------------------------------------------- group files by timeseries
    # chunk files that are within same timeseries
    mwcch_chunks = mwcch_chunk.chunk_files_by_timerange(mwcch_files, n_frames, msg_res, gap=gap)
    print(f"number of timeseries: {len(mwcch_chunks)}")

    # ---------------------------------------------------------------- loop over timeseries groups
    # loop over mwcch chunks
    for g, group in enumerate(mwcch_chunks):
        if g % 1000 == 0:
            print(f"---- processing timeseries {g}/{len(mwcch_chunks)}", flush=True)

        try:
            # ------------------------------------------------------------ read last MWCC-H
            # read in mwcch_file of last frame
            mwcch_last_frame = mwcch_read.read(group[0], variables=["POH", "hail_class"])
            
            # ------------------------------------------------------------ get label
            # set label to maximum hail class within domain
            max_hail_class = mwcch_read.max_hail_class(mwcch_last_frame.hail_class.values, min_pixel=min_pix)

            # define output path for this label
            path_label = os.path.join(output_path, f"{max_hail_class}_{mwcch_read.convert_hail_class(max_hail_class, to='name')}")
            if not os.path.exists(path_label):
                os.makedirs(path_label)

            # ------------------------------------------------------------ create MSG timeseries
            # read in MSG time series ending in mwcc-h timestamp

            # get end time of overpass
            mwcch_end = mwcch_last_frame.end_scan

            # get corresponding MSG time series
            msg_timeseries = collect_MSG_timeseries(mwcch_end, msg_res, n_frames)

            # ------------------------------------------------------------ get crop extent
            # get center of mass of max hail class area
            cg_lon, cg_lat, minlon, maxlon, minlat, maxlat = \
                mwcch_crop.get_crop_extent_over_maxhailarea(mwcch_last_frame, cropsize, min_pixel=min_pix)

            # ------------------------------------------------------------ crop over hail area or overpass
            # crop dataset over hail area or overpass area
            msg_timeseries = msg_timeseries.sel(lon=slice(minlon, maxlon), lat=slice(minlat, maxlat))
            
            # add global attributes describing the data
            msg_timeseries = add_attributes(msg_timeseries, cg_lon, cg_lat)

            # ------------------------------------------------------------ save to file
            # define output filename
            dt_end = msg_timeseries.end_time
            filepath = os.path.join(path_label, f"{dt_end}_res{msg_res}min_{n_frames}frames_cropsize{cropsize}.nc")

            # save to given filepath
            msg_timeseries.to_netcdf(filepath)
        
        except:
            print(f"Error processing timeseries {g}/{len(mwcch_chunks)}")
            continue


# %%
if __name__ == "__main__":

    start_script_at = datetime.datetime.now()

    # study settings
    years = np.arange(2006, 2024, 1)
    months = np.arange(4, 10, 1)
    print("study period: ", years, months)
    
    # MWCC-H filters
    area_threshold = 30
    print("area threshold: ", area_threshold)

    # time series settings
    msg_res = 15
    n_frames = 4
    gap = 15
    print(f"MSG timeseries settings: resolution {msg_res}, n_frames {n_frames}, gap {gap}")

    # cropping settings
    cropsize = 128
    min_pix = 5
    print(f"crop settings: cropsize {cropsize}, min_pix {min_pix}")

    # construct dataset
    path = f"/net/merisi/pbigalke/data/labelled_MSG_timeseries"
    construct_labelled_MSG_timeseries(path, years, months, area_threshold, msg_res, n_frames, gap, cropsize, min_pix)

    print("total runtime: ", datetime.datetime.now() - start_script_at)

# %%
