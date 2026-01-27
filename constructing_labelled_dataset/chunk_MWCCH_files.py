# This script contains a function to chunk MWCCH files into groups based on MSG timeseries length. 
# It is used in the procedure of the creation of labelled MSG timeseries datasets (see construct_labelled_timeseries.py).

# %%
import numpy as np
import sys
sys.path.append('..')
import readers.read_processed_MWCCH as mwcch
import helpers.collect_matching_files as match


# %%
# main method to use for chunking MWCCH files into groups by MSG timeseries length
def chunk_files_by_timerange(files, n_frames, msg_res, gap=15, start_match="following", chunk_match="previous"):
    """
    Chunk MWCCH files that lie within MSG timeseries length (given by n_frames*msg_res) to enable construction of labelled MSG timeseries.
    The chunks are created going backwards in time, starting with the newest MWCCH file. 
    This ensures that the MSG timeseries later created on the basis of the chunks are ENDING with the MWCCH hail information. 
    All previous MWCCH files that lie within the timeseries length are grouped into the same chunk.

    : param files: list of MWCCH file paths
    : param n_frames: number of frames in the time series
    : param msg_res: temporal resolution of MSG data in minutes
    : param gap: minimum gap in minutes between the end of one time series and the start of the next
    : param start_match: which MSG timestamp to use as reference for the end of the time series ("following", "previous", "closest"). 
                         If "following", the time series will end at the next MSG timestamp after the MWCCH overpass,
                         if "previous" it will end at the previous MSG timestamp before the MWCCH overpass, 
                         if "closest" it will end at the closest MSG timestamp to the MWCCH overpass (either before or after).
    : param chunk_match: In which direction to check if MWCCH overpass lies within same MSG timeseries so that it grouped together 
                         with previous MWCCH overpasses ("following", "previous", "closest").
    
    : return chunks: list of lists, where each sublist contains file paths that belong to the same time series chunk
    """
    # Parse timestamps of scanning end time
    files_with_timestamps = [(file, mwcch.get_scan_datetime_from_mwcch_filepath(file, which="end")) for file in files]

    # sort files by timestamp in descending order
    files_with_timestamps.sort(key=lambda x: x[1], reverse=True)

    # Chunk files based on the specified time range
    chunks = []
    current_chunk = []
    current_start_time = None
    current_end_time = None
    timeseries_length = np.timedelta64(n_frames-1, 'm') * msg_res
    gap_length = np.timedelta64(gap, 'm')

    # loop over all files
    for file, timestamp in files_with_timestamps:

        # get corresponding MSG timestamps if this would be timeseries' last frame
        msg_timestamp_if_last_frame = match.get_closest_MSG_timestamps(timestamp, which=start_match, msg_res=msg_res)
        
        # get corresponding MSG timestamps to check if this file is within the time range to be chunked with previous files
        msg_timestamp_chunk = match.get_closest_MSG_timestamps(timestamp, which=chunk_match, msg_res=msg_res)

        # check if this is the first file
        if current_start_time is None:
            # set current start time to corresponding MSG timestamp of the first file
            current_start_time = msg_timestamp_if_last_frame
            # set respective end time
            current_end_time = current_start_time - timeseries_length

            # add file to current chunk
            current_chunk.append(file)

        else:

            # check if file is within range of current chunk
            if (current_start_time - msg_timestamp_chunk) <= timeseries_length:
                # add file to current chunk
                current_chunk.append(file)

            else:
                # file is out of bound for previous chunk

                # if current chunk is not empty, add it to final list
                if current_chunk:
                    chunks.append(current_chunk)

                # check if gap of current file to previous timeseries is large enough to start new chunk
                if (current_end_time - msg_timestamp_if_last_frame) >= gap_length:
                    current_chunk = [file]
                    # set new start time to corresponding MSG timestamp of the current file
                    current_start_time = msg_timestamp_if_last_frame
                    # set respective end time
                    current_end_time = current_start_time - timeseries_length
                else:
                    # do not start new chunk
                    current_chunk = []

    # if list of current chunk is not empty, add it to final list
    if current_chunk:
        chunks.append(current_chunk)

    return chunks
