# %%
import numpy as np
import os
import sys
import matplotlib.pyplot as plt
import matplotlib as mpl
sys.path.append('..')
import MWCCH_file_lists_for_studies as mwcch_list
import readers.read_processed_MWCC_H as mwcch_read
import matching_data.collect_matching_files as match


# %%
def chunk_files_by_timerange(files, n_frames, msg_res, gap=15, start_match="following", chunk_match="previous"):
    """
    Chunk MWCCH files that lie within a specified time range (given by n_frames*msg_res).
    Each chunk will contain files that fall within the time range of the described MSG time series.
    Parameters:
    - files: list of MWCCH file paths
    - n_frames: number of frames in the time series
    - msg_res: temporal resolution of MSG data in minutes
    - gap: minimum gap in minutes between the end of one time series and the start of the next
    - start_match: method to match the start time of the time series to MSG timestamps ("following", "previous", "closest")
    - chunk_match: method to match the files to the time range of the time series ("following", "previous", "closest")
    Returns:
    - chunks: list of lists, where each sublist contains file paths that belong to the same time series chunk
    """
    # Parse timestamps of scanning end time
    files_with_timestamps = [(file, mwcch_read.get_scan_datetime_from_mwcch_filepath(file, which="end")) for file in files]

    # sort files by timestamp in descending order
    files_with_timestamps.sort(key=lambda x: x[1], reverse=True)

    # Chunk files based on the specified time range
    chunks = []
    current_chunk = []
    current_start_time = None
    current_end_time = None
    timeseries_length = np.timedelta64((n_frames-1)*msg_res, 'm')
    gap_length = np.timedelta64(gap, 'm')

    # loop over all files
    for file, timestamp in files_with_timestamps:

        # get corresponding MSG timestamps if this would be timeseries start
        msg_last_frame = match.get_closest_MSG_timestamps(timestamp, which=start_match, msg_res=msg_res)
        
        # get corresponding MSG timestamps to check if this file is within the time range
        msg_chunk_match = match.get_closest_MSG_timestamps(timestamp, which=chunk_match, msg_res=msg_res)


        # check if this is the first file
        if current_start_time is None:
            # set current start time to corresponding MSG timestamp of the first file
            current_start_time = msg_last_frame
            # set respective end time
            current_end_time = current_start_time - timeseries_length

            # add file to current chunk
            current_chunk.append(file)

        #check if file is within range of current chunk
        elif (current_start_time - msg_chunk_match) <= timeseries_length:
            # add file to current chunk
            current_chunk.append(file)

        else:
            # file is out of bound for previous chunk

            # if current chunk is not empty, add it to final list
            if current_chunk:
                chunks.append(current_chunk)

            # check if gap of current file to previous timeseries is large enough to start new chunk
            if (current_end_time - msg_last_frame) >= gap_length:
                current_chunk = [file]
                # set new start time to corresponding MSG timestamp of the current file
                current_start_time = msg_last_frame
                # set respective end time
                current_end_time = current_start_time - timeseries_length
            else:
                # do not start new chunk
                current_chunk = []

    if current_chunk:
        chunks.append(current_chunk)

    return chunks

# %%
# some plotting functions to analyze chunking of MWCCH files
def plot_numer_of_MWCCH_chunks_over_gap_per_areathresh(mwcch_path, years, months, n_frames, msg_res, plotpath, area_thresholds, gaps, start_match, chunk_match):
    fig, axes = plt.subplots(2, 4, figsize=(15, 10))
    plot_colors = ['r', 'g', 'b', 'c', 'm', 'y']
    n_max_files = 0
    
    for t, thresh in enumerate(area_thresholds):
        ax = axes[t//4, t%4]

        count_line = 0
        for start in start_match:
            for chunk in chunk_match:
                n_files = []
                n_chunks = []
                for g in gaps:
                    mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_path, years, months, thresh)
                    chunks = chunk_files_by_timerange(mwcch_files, n_frames, msg_res, start_match=start, chunk_match=chunk, gap=g)
                    n_files.append(len(mwcch_files))
                    n_chunks.append(len(chunks))
                    if t == 0:
                        n_max_files = len(mwcch_files)
                
                if count_line == 0:
                    ax.set_title(f"area thresh = {thresh}%")
                    
                ax.plot(gaps, n_chunks, label=f"({start}/{chunk})", color=plot_colors[count_line])
                count_line += 1
        # draw x label if in last row
        if t//4 == 1:
            ax.set_xlabel("Gap between time series [min]")
        # draw y label if in first column
        if t%4 == 0:
            ax.set_ylabel("# chunks")
        ax.set_ylim(95, 350)
        ax.grid()

        # draw legend only for the last subplot and move it outside the plot to the right
        if t == len(area_thresholds)-1:
            ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    # turn off last subplot
    ax = axes[-1, -1]
    ax.axis('off')

    fig.suptitle(f"Chunking {n_max_files} MWCCH files")
    plt.savefig(f"{plotpath}/chunking_mwcch_files_per_gap_for_diff_thresh.png")
    plt.show()
    plt.close()

def plot_number_of_MWCCH_chunks_over_areathreh_per_gap(mwcch_path, years, months, n_frames, msg_res, plotpath, area_thresholds, gaps, start_match, chunk_match):
    fig, axes = plt.subplots(2, 3, figsize=(15, 10))
    plot_colors = ['r', 'g', 'b', 'c', 'm', 'y']
    n_max_files = 0
    for g, gap in enumerate(gaps):
        ax = axes[g//2, g%2]

        count_line = 0
        for start in start_match:
            for chunk in chunk_match:
                n_files = []
                n_chunks = []
                for t, thresh in enumerate(area_thresholds):
                    mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_path, years, months, thresh)
                    chunks = chunk_files_by_timerange(mwcch_files, n_frames, msg_res, start_match=start, chunk_match=chunk, gap=gap)
                    n_files.append(len(mwcch_files))
                    n_chunks.append(len(chunks))
                    if t == 0:
                        n_max_files = len(mwcch_files)
                
                if count_line == 0:
                    ax.set_title(f"gap = {gap}min")
                
                ax.plot(area_thresholds, n_chunks, label=f"({start}/{chunk})", color=plot_colors[count_line])
                count_line += 1
        # draw x label if in last row
        if g//2 == 1:
            ax.set_xlabel("Area threshold [%]")
        # draw y label if in first column
        if g%2 == 0:
            ax.set_ylabel("# chunks")
        # ax.set_ylim(95, 350)
        ax.grid()
        ax.set_ylim(90, 500)

        # draw legend only for the last subplot and move it outside the plot to the right
        if g == len(gaps)-1:
            ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
            
            # plot total number of files in upper right subplot
            # get axes for lower right plot
            ax = axes[0, 2]
            ax.plot(area_thresholds, n_files, label="total", color='k')
            ax.set_title("total amount of files")
            ax.set_ylabel("# files")
            ax.grid()

    # turn off last subplot
    ax = axes[-1, -1]
    ax.axis('off')

    fig.suptitle(f"Chunking {len(n_max_files)} MWCCH files")
    plt.savefig(f"{plotpath}/chunking_mwcch_files_per_areathresh_for_diff_gaps.png")
    plt.show()
    plt.close()

def plot_chunksize_distribution_per_area_thresh(mwcch_path, years, months, n_frames, msg_res, plotpath, area_thresholds):
    fig, ax = plt.subplots(1, figsize=(6, 4))
    ax.set_title(f"gap: 15 min, timeseries length: {n_frames} frames")
    
    max_expected_chunk_size = 5
    counts = np.zeros((len(area_thresholds), max_expected_chunk_size))

    for t, thresh in enumerate(area_thresholds):

        # read in all mwcc-h files for study settings
        mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_path, years, months, thresh)
        # group according to time range
        chunks = chunk_files_by_timerange(mwcch_files, n_frames, msg_res, 
                                            start_match="following", chunk_match="previous", gap=15)
        
        # get chunk sizes
        chunk_sizes = np.array([len(chunk) for chunk in chunks])

        # sizes occuring in this setting
        sizes = np.unique(chunk_sizes)

        for size in sizes:
            counts[t, size-1] = np.count_nonzero(chunk_sizes == size)

    # plot heat map with imshow of chunk sizes per area threshold
    c = ax.imshow(counts, cmap='viridis', aspect='auto', interpolation='nearest', origin='lower', 
                  norm=mpl.colors.LogNorm(vmin=1, vmax=280))

    # plot colorbar
    fig.colorbar(c, ax=ax, orientation='vertical', label='number of chunks')

    # write count as number in each cell of the heatmap
    for i in range(len(area_thresholds)):
        for j in range(max_expected_chunk_size):
            ax.text(j, i, int(counts[i, j]), ha='center', va='center', color='white')

    # write text with total number of chunks in upper right corner
    ax.text(0.95, 0.95, f"# chunks: {int(np.sum(counts))}", transform=ax.transAxes, ha='right', va='top')

    # set xticks as chunk sizes
    ax.set_xticks(np.arange(0, max_expected_chunk_size), labels=np.arange(1, max_expected_chunk_size+1))
    ax.set_xlabel("chunk size")

    # set yticks as area thresholds
    ax.set_yticks(np.arange(0, len(area_thresholds)), labels=area_thresholds)
    ax.set_ylabel("area threshold [%]")

    plt.savefig(f"{plotpath}/chunk_size_distribution_per_area_thresh_{n_frames}frames.png")
    plt.show()
    plt.close()

def plot_number_of_MWCCH_chunks_over_n_frames_and_gap_per_areathresh(mwcch_path, years, months, n_frames, msg_res, 
                                                                     plotpath, area_thresholds, gaps):
    
    # find number of subplots needed
    n_subplots = len(area_thresholds)
    n_cols = np.ceil(np.sqrt(n_subplots)).astype(int)
    n_rows = np.ceil(n_subplots / n_cols).astype(int)
    
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 15))
    
    for t, thresh in enumerate(area_thresholds[:1]):
        mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_path, years, months, thresh)
        ax = axes[t//2, t%2]

        # create container to store total number of chunks
        n_chunks = np.zeros((len(n_frames), len(gaps)))

        # loop over number of frames
        for n, frames in enumerate(n_frames):
            # loop over gaps
            for g, gap in enumerate(gaps):

                    chunks = chunk_files_by_timerange(mwcch_files, frames, msg_res, gap=gap)
                    n_chunks[n, g] = len(chunks)
                
        ax.set_title(f"area thresh = {thresh}%")
        # plot number of chunks as heatmap with n_frames on y axis and gaps on x axis
        c = ax.imshow(n_chunks, cmap='viridis', aspect='auto', interpolation='nearest', origin='lower', 
                      norm=mpl.colors.LogNorm(vmin=1, vmax=500))
        # plot colorbar
        fig.colorbar(c, ax=ax, orientation='vertical', label='number of timeseries')

        # draw x label if in last row
        if t//2 == 1:
            ax.set_xlabel("gap between timeseries [min]")

        # draw y label if in first column
        if t%2 == 0:
            ax.set_ylabel("number of frames")

        # draw second y axis for corresponding timeseries length in minutes in last column
        if t%2 == n_cols-1:
            ax2 = ax.twinx()
            ax2.set_ylabel("timeseries length [min]")
            ax2.set_yticks(n_frames, labels=n_frames*msg_res)

    # turn off subplots that are not used
    for i in range(n_subplots, n_rows*n_cols):
        ax = axes[i//n_cols, i%n_cols]
        ax.axis('off')

    if plotpath is not None:
        plt.savefig(f"{plotpath}/chunking_mwcch_files_per_areathresh_for_diff_gaps.png")
        plt.close()
    else:
        plt.show()
        plt.close()

# %%
if __name__ == "__main__":

    mwcch_path = mwcch_read.MWCCH_MSGGRID_PATH
    plotpath = None #"/net/merisi/pbigalke/plots/data_investigation/constructing_dataset/chunking_MWCCH_files"
    # if not os.path.exists(plotpath):
    #     os.makedirs(plotpath)

    # study period settings
    years = [2022]
    months = [6]

    # time series settings
    msg_res = [5, 15]
    duration_min = np.arange(30, 241, 15)

    # area thresholds and gaps to test
    area_thresholds = np.arange(0, 70, 10)
    
    start_match="following"
    chunk_match="previous"


    ###### plot chunking of MWCCH files per number of frames and gap for different area thresholds #######
    for res in msg_res[:1]:
        n_frames = duration_min / res
        gaps = np.arange(-30, 30, res)

        plot_number_of_MWCCH_chunks_over_n_frames_and_gap_per_areathresh(mwcch_path, years, months, n_frames, res, 
                                                                        plotpath, area_thresholds, gaps)

    # ###### plot chunking of MWCCH files per gap for different area thresholds #######
    # plot_number_of_MWCCH_chunks_over_gap_per_areathresh(mwcch_path, years, months, n_frames, msg_res, plotpath, 
    #                                                    area_thresholds, gaps, start_match, chunk_match)
    
    # # ####### plot chunking of MWCCH files per area thresh for different gaps #######
    # plot_number_of_MWCCH_chunks_over_area_thresh_per_gap(mwcch_path, years, months, n_frames, msg_res, plotpath, 
    #                                                    area_thresholds, gaps, start_match, chunk_match)
    

    # # ####### plot chunk size per area thresh #######
    # for n_frames in np.arange(4, 9, 1):
    #     plot_chunksize_distribution_per_area_thresh(mwcch_path, years, months, n_frames, msg_res, plotpath, area_thresholds)

# %%
