# With this script one can generate some plots for investigating the influence of different settings 
# on the number of MWCCH chunks (corresponding to labelled timeseries). 
# Some settings that considered are area threshold, number of frames in timeseries, gap between timeseries, etc.
# WARNING: some of these plotting functions have not yet been adapted to reading the MWCCH files

# %%
import numpy as np
import sys
import matplotlib.pyplot as plt
import matplotlib as mpl
sys.path.append('..')
import MWCCH_file_lists_for_studies as mwcch_list
from chunk_MWCCH_files import chunk_files_by_timerange


# %%
# some plotting functions to analyze chunking of MWCCH files
# into MSG timeseries depending on different parameters
def plot_numer_of_MWCCH_chunks_over_gap_per_areathresh(mwcch_path, years, months, n_frames, msg_res, plotpath, 
                                                       area_thresholds, gaps, start_match, chunk_match):
    """
    Plot number of MWCCH chunks over different gaps, each subplot for a specific area threshold.
    WARNING: this plotting function has not yet been adapted to reading the MWCCH files from S3 bucket!

    :param mwcch_path: Path to MWCCH files
    :param years: List of years to include
    :param months: List of months to include
    :param n_frames: Number of frames in each timeseries
    :param msg_res: Resolution of MSG data in minutes
    :param plotpath: Path to save the plot
    :param area_thresholds: List of area thresholds to consider
    :param gaps: List of gaps to consider
    :param start_match: List of start match criteria
    :param chunk_match: List of chunk match criteria
    """
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

def plot_number_of_MWCCH_chunks_over_areathreh_per_gap(mwcch_path, years, months, n_frames, msg_res, plotpath, 
                                                       area_thresholds, gaps, start_match, chunk_match):
    """
    Plot number of MWCCH chunks over different area thresholds, each subplot for a specific gap.
    WARNING: this plotting function has not yet been adapted to reading the MWCCH files from S3 bucket!

    :param mwcch_path: Path to MWCCH files
    :param years: List of years to include
    :param months: List of months to include
    :param n_frames: Number of frames in each timeseries
    :param msg_res: Resolution of MSG data in minutes
    :param plotpath: Path to save the plot
    :param area_thresholds: List of area thresholds to consider
    :param gaps: List of gaps to consider
    :param start_match: List of start match criteria
    :param chunk_match: List of chunk match criteria
    """
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
    """
    Plot distribution of chunk sizes per area threshold as heatmap.
    WARNING: this plotting function has not yet been adapted to reading the MWCCH files from S3 bucket!
    
    :param mwcch_path: Path to MWCCH files
    :param years: List of years to include
    :param months: List of months to include
    :param n_frames: Number of frames in each timeseries
    :param msg_res: Resolution of MSG data in minutes
    :param plotpath: Path to save the plot
    :param area_thresholds: List of area thresholds to consider
    """
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

def plot_number_of_MWCCH_chunks_over_n_frames_and_gap_per_areathresh(mwcch_bucket, years, months, n_frames, msg_res, 
                                                                     plotpath, area_thresholds, gaps, n_rows, n_cols,
                                                                     vmax=42500, vmin=5000, bin_width=2500):
    """
    Plot number of MWCCH chunks over different n_frames and gaps as heatmap, each subplot for a specific area threshold.

    :param mwcch_bucket: S3 bucket name where MWCCH files are stored
    :param years: List of years to include
    :param months: List of months to include
    :param n_frames: List of number of frames in each timeseries
    :param msg_res: Resolution of MSG data in minutes
    :param plotpath: Path to save the plot
    :param area_thresholds: List of area thresholds to consider
    :param gaps: List of gaps between timeseries in minutes
    :param n_rows: Number of rows in the subplot grid
    :param n_cols: Number of columns in the subplot grid
    :param vmax: Maximum value for color scale
    :param vmin: Minimum value for color scale
    :param bin_width: Width of bins for color scale
    """
    # find number of subplots needed
    n_subplots = len(area_thresholds)
    
    # create figure and axes
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(3*n_cols, 3*n_rows))
    
    for t, thresh in enumerate(area_thresholds):
        mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_bucket, years, months, days, thresh)
        idx_row = t // n_cols
        idx_col = t % n_cols
        ax = axes[idx_row, idx_col]

        # create color map
        cmap = plt.cm.jet  # define the colormap
        # extract all colors from the .jet map
        cmaplist = [cmap(i) for i in range(cmap.N)]

        # create the new map
        cmap = mpl.colors.LinearSegmentedColormap.from_list(
            'Custom cmap', cmaplist, cmap.N)

        # define the bins and normalize
        bounds = np.arange(vmin, vmax+1, bin_width)
        norm = mpl.colors.BoundaryNorm(bounds, cmap.N)

        # create container to store total number of chunks
        n_chunks = np.zeros((len(n_frames), len(gaps)))

        # loop over number of frames
        for n, frames in enumerate(n_frames):
            # loop over gaps
            for g, gap in enumerate(gaps):
                    if t == 0 and g == 0 and n == 0:
                        print("max number of files:", len(mwcch_files))

                    chunks = chunk_files_by_timerange(mwcch_files, frames, msg_res, gap=gap)
                    n_chunks[n, g] = len(chunks)

        # plot number of chunks as heatmap with n_frames on y axis and gaps on x axis
        c = ax.imshow(n_chunks, cmap=cmap, norm=norm, aspect='auto', interpolation='nearest', origin='lower')
        
        # draw title for each subplot
        ax.set_title(f"area thresh = {thresh}%")

        # draw x label if in last row
        xticks = np.arange(len(gaps)) if msg_res == 15 else np.arange(len(gaps))[::2]
        if idx_row == n_rows-1:
            ax.set_xlabel("gap between timeseries [min]")
            ax.set_xticks(xticks, labels=gaps[xticks], rotation=45, ha='center')
        else:
            # turn off x tick labels
            ax.set_xticks(xticks, labels=[])

        # draw y label if in first column
        yticks = np.arange(len(n_frames))[::2]
        if idx_col == 0:
            ax.set_ylabel("number of frames")
            ax.set_yticks(yticks, labels=n_frames[yticks])
        else:
            # turn off y ticks
            ax.set_yticks(yticks, labels=[])

        # draw second y axis for corresponding timeseries length in minutes in last column and last subplot
        if idx_col == n_cols-1 or t == n_subplots-1:
            ax2 = ax.twinx()
            ax2.set_ylabel("timeseries length [min]")
            ax2.set_yticks(yticks, labels=n_frames[yticks] * msg_res)
        else:
            # turn off y ticks
            ax2 = ax.twinx()
            ax2.set_yticks(yticks, labels=[])

    # turn off subplots that are not used
    for i in range(n_subplots, n_rows*n_cols):
        ax = axes[i//n_cols, i%n_cols]
        ax.axis('off')
        # draw colorbar here
        fig.colorbar(c, ax=ax, orientation='vertical', label='number of timeseries')

    if plotpath is not None:
        plt.savefig(f"{plotpath}/num_timeseries_per_areathresh_nframes_gap_res{res}_years{years[0]}-{years[-1]}.png")
        plt.close()
    else:
        plt.show()
        plt.close()

def plot_number_of_MWCCH_chunks_over_n_frames_and_gap_for_specific_areathresh(mwcch_bucket, years, months, n_frames, msg_res, 
                                                                              plotpath, area_thresh, gaps, vmax=42500, vmin=5000, 
                                                                              bin_width=2500):  
    """
    Plot number of MWCCH chunks over different n_frames and gaps as heatmap for one specific area threshold.

    :param mwcch_bucket: S3 bucket name where MWCCH files are stored
    :param years: List of years to include in the study
    :param months: List of months to include in the study
    :param n_frames: List of number of frames to consider
    :param msg_res: Message resolution in minutes
    :param plotpath: Path to save the plot
    :param area_thresh: Area threshold to filter MWCCH files
    :param gaps: List of gaps between timeseries in minutes
    :param vmax: Maximum value for color scale
    :param vmin: Minimum value for color scale
    :param bin_width: Width of bins for color scale
    """  
    # create figure and axes
    fig, ax = plt.subplots(1, 1, figsize=(15, 10))

    mwcch_files = mwcch_list.read_mwcch_files_for_study_settings(mwcch_bucket, years, months, days, area_thresh)

    # create color map
    cmap = plt.cm.jet  # define the colormap
    # extract all colors from the .jet map
    cmaplist = [cmap(i) for i in range(cmap.N)]

    # create the new map
    cmap = mpl.colors.LinearSegmentedColormap.from_list(
        'Custom cmap', cmaplist, cmap.N)

    # define the bins and normalize
    bounds = np.arange(vmin, vmax+1, bin_width)
    norm = mpl.colors.BoundaryNorm(bounds, cmap.N)

    # create container to store total number of chunks
    n_chunks = np.zeros((len(n_frames), len(gaps)))

    # loop over number of frames
    for n, frames in enumerate(n_frames):
        # loop over gaps
        for g, gap in enumerate(gaps):
                # get number of chunks
                chunks = chunk_files_by_timerange(mwcch_files, frames, msg_res, gap=gap)
                n_chunks[n, g] = len(chunks)

    # plot number of chunks as heatmap with n_frames on y axis and gaps on x axis
    c = ax.imshow(n_chunks, cmap=cmap, norm=norm, aspect='auto', interpolation='nearest', origin='lower')

    # write number on chunks in each cell
    for i in range(len(n_frames)):
        for j in range(len(gaps)):
            ax.text(j, i, int(n_chunks[i, j]), ha='center', va='center', color='white')
    
    # draw title for each subplot
    ax.set_title(f"area thresh = {area_thresh}%, msg res = {msg_res}min")

    # draw x label if in last row
    xticks = np.arange(len(gaps)) if msg_res == 15 else np.arange(len(gaps))[::2]
    ax.set_xlabel("gap between timeseries [min]")
    ax.set_xticks(xticks, labels=gaps[xticks], rotation=45, ha='center')

    # draw y label if in first column
    yticks = np.arange(len(n_frames))[::2]
    ax.set_ylabel("number of frames")
    ax.set_yticks(yticks, labels=n_frames[yticks])

    # draw second y axis for corresponding timeseries length in minutes in last column and last subplot
    ax2 = ax.twinx()
    ax2.set_ylabel("timeseries length [min]")
    ax2.set_yticks(yticks, labels=n_frames[yticks] * msg_res)

    # draw colorbar with an offset so that it does not overlap with the second y axis
    fig.colorbar(c, ax=ax, orientation='vertical', label='number of timeseries', pad=0.1)

    if plotpath is not None:
        plt.savefig(f"{plotpath}/num_timeseries_areathresh{area_thresh}_nframes_gap_res{res}_years{years[0]}-{years[-1]}.png")
        plt.close()
    else:
        plt.show()
        plt.close()

# %%
if __name__ == "__main__":

    # Here we did some plotting to test the chunking of MWCCH files into MSG timeseries
    # depending on different parameters such as area threshold, number of frames, gap between timeseries

    # bucket and plot path settings
    mwcch_bucket = "mwcch-hail-regrid-msg"
    plotpath = "plots" #"/net/merisi/pbigalke/plots/data_investigation/constructing_dataset/chunking_MWCCH_files"
    # if not os.path.exists(plotpath):
    #     os.makedirs(plotpath)

    # study period settings
    # years = np.arange(2013, 2024, 1)
    years = np.arange(2006, 2024, 1)
    months = np.arange(4, 10, 1)
    days = np.arange(1, 32, 1)

    # time series settings
    msg_res = [5, 15]
    duration_min = np.arange(30, 241, 15)

    # area thresholds and gaps to test
    area_thresholds = np.arange(0, 70, 10)

    ###### plot chunking of MWCCH files per number of frames and gap for different area thresholds #######
    for res in msg_res:
        n_frames = np.array(duration_min / res).astype(int)
        gaps = np.arange(-30, 30+res, res)

        # plot_number_of_MWCCH_chunks_over_n_frames_and_gap_per_areathresh(mwcch_bucket, years, months, n_frames, res, 
        #                                                                 plotpath, area_thresholds, gaps, n_rows=2, n_cols=4,
        #                                                                 vmax=42500, vmin=5000, bin_width=2500)

        for thresh in area_thresholds:
             plot_number_of_MWCCH_chunks_over_n_frames_and_gap_for_specific_areathresh(mwcch_bucket, years, months, n_frames, res, 
                                                                     plotpath, thresh, gaps, vmax=42500, vmin=5000, bin_width=2500)


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
