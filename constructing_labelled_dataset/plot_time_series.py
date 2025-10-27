# %%
import numpy as np
import pandas as pd
import xarray as xr
from scipy import ndimage as ndi
import glob
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.gridspec import GridSpec
import os
import sys
sys.path.append('..')
import construct_labelled_timeseries as clt
import readers.read_MSG as msg_read
import readers.read_processed_MWCC_H as mwcch_read
import matching_data.collect_matching_files as match
import helpers.datetime_helper as hlp
from plotting.mpl_style import LABELSIZE, TICKSIZE, TRANSFORM, CMAP_MSG_GREY
import plotting.plot_orography_and_map as map_plt
import plotting.plot_MSG as msg_plt
import plotting.plot_MWCC_H as mwcch_plt
from config.domain_info import domain_expats

# %%
def plot_timeseries_examples_for_each_hailclass(timeseries_path, channel, n_frames, n_examples=5, output_path=None):

    # get all hail classes
    hail_class_names = mwcch_read.get_hail_classes(type="name")

    # loop over hail classes
    for h, hail in enumerate(hail_class_names):

        # get all MSG time series files
        label_timeseries = sorted(glob.glob(f"{timeseries_path}/{h}_{hail}/*.nc"))

        # pick random 5 examples
        label_timeseries = np.random.choice(label_timeseries, n_examples)
        # check if there are examples in this hail class
        if len(label_timeseries) > 0:

            # set up figure
            fig = plt.figure(figsize=(n_frames*2, n_examples*2)) #, layout="constrained")

            # devide figure in axes for colorbars and plot
            gs = GridSpec(n_examples, n_frames, figure=fig)

            # loop over all time series
            for row, tms in enumerate(label_timeseries):

                # read in MSG_timeseries
                data_timeserie = msg_read.read(tms)

                # get extent of crop [minlon, maxlon, minlat, maxlat]]
                extent = [data_timeserie.lon.values[0], data_timeserie.lon.values[-1], data_timeserie.lat.values[0], data_timeserie.lat.values[-1]]

                # loop over timestamps
                for col, time in enumerate(data_timeserie.time.values):

                    # get data of this timestamp
                    data_timestamp = data_timeserie.sel(time=time)
                    
                    # get axis 
                    ax = fig.add_subplot(gs[row, col], projection=TRANSFORM)
                    # set title
                    dt_str = hlp.get_datetimestring_from_npdatetime(time)
                    title = f"{dt_str[:8]} - {dt_str[-4:-2]}:{dt_str[-2:]}" if col == 0 else f"{dt_str[-4:-2]}:{dt_str[-2:]}"
                    ax.set_title(title)

                    # get data of channel
                    msg_lons = data_timestamp.lon.values
                    msg_lats = data_timestamp.lat.values

                    # select data of this timestamp and channel
                    if "-" in channel:
                        chan1 = channel.split("-")[0]
                        chan2 = channel.split("-")[1]
                        msg_tb = data_timestamp[chan1].values - data_timestamp[chan2].values
                        vmin, vmax = -60, 5
                        cmap = msg_plt.get_msg_cmap(channel, vmin=vmin, vmax=vmax)
                    else:
                        msg_tb = data_timestamp[channel].values
                        vmin, vmax = 200, 270
                        cmap = msg_plt.get_msg_cmap(channel)
                    
                    # draw map
                    msg_plt.draw_map(ax, extent=extent, mode="light")

                    # draw grid
                    # define if ticks are drawn (yticks only for first col and xticks only for last row)
                    xticks = True if row == n_examples-1 else False
                    yticks = True if col == 0 else False
                    msg_plt.draw_grid(ax, xticks=xticks, yticks=yticks)

                    # plot msg channel over map
                    msg_plt.plot_msg_data(ax, msg_lons, msg_lats, msg_tb, 
                                cmap=cmap, vmin=vmin, vmax=vmax)

                
            # set title
            fig.suptitle(f"{hail} examples")
            
            # # save to file
            plt.tight_layout()

            if output_path is not None:
                # define output name
                if not os.path.exists(output_path):
                    os.makedirs(output_path)
                filename = f"{output_path}/{hail}_{n_examples}example_timeseries_{n_frames}frames_{channel}.png"
                plt.savefig(filename, bbox_inches='tight', transparent=True)
                plt.close()
            else:
                plt.show()
                plt.close()

# %%
def plot_hail_class_distribution(timeseries_folder, output_name=None, figsize=(8, 5)):
    
    f, ax1 = plt.subplots(1)
    f.set_figheight(figsize[1])
    f.set_figwidth(figsize[0])
    ax1.set_title("distribution of hail classes")

    # get hail class names
    hail_class_names = mwcch_read.get_hail_classes(type="name")

    # get total amount of files
    N_total = len(glob.glob(f"{timeseries_folder}/*/*.nc"))
    p_nohail = 0
    p_hail = 0

    # loop over hail class
    for h, hail in enumerate(hail_class_names):

        # get number of this hail class
        n_class = len(glob.glob(f"{timeseries_folder}/{h}_{hail}/*.nc"))
        perc_class = n_class / N_total * 100 

        # plot bar for this class
        ax1.bar(h, n_class, -0.8, 
                color=mwcch_plt.hail_class_colors_list[int(h)], 
                align="center")

        # add percentage above max poh bars
        position = 1 if n_class == 0 else n_class
        ax1.text(h, position, f'{perc_class:.2f}', fontsize=12, 
                    horizontalalignment='center', verticalalignment='bottom')
        
        # add to hail / non hail counter
        if h <= 1:
            p_nohail += perc_class
        else:
            p_hail += perc_class

    # format x axes
    x_center = np.arange(0, len(hail_class_names), 1)
    ax1.set_xticks(x_center, labels=hail_class_names, rotation=45, ha='right')
    ax1.set_xlim(x_center[0]-0.5, x_center[-1] + 0.5)
    ax1.set_ylabel("# of timeseries")
    ax1.grid(False)

    # add total number of files in upper right corner of axes
    ax1.text(0.95, 0.95, f"# total: {N_total}\nno hail: {p_nohail:.0f}%\nhail: {p_hail:.0f}%", fontsize=12, 
                horizontalalignment='right', verticalalignment='top', 
                transform=ax1.transAxes)
    #
    plt.tight_layout()

    if output_name is not None:
        plt.savefig(output_name, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
        plt.close()

def plot_yearly_hailclass_distribution(timeseries_folder, years, output_name=None, figsize=(10, 5)):
    
    f, ax1 = plt.subplots(1)
    f.set_figheight(figsize[1])
    f.set_figwidth(figsize[0])
    ax1.set_title("distribution of data over years")

    # get total amount of files
    N_total = len(glob.glob(f"{timeseries_folder}/*/*.nc"))

    # get hail class names
    hail_class_names = mwcch_read.get_hail_classes(type="name")

    bottom = np.zeros(len(years))
    # loop over hail class
    for h, hail in enumerate(hail_class_names):

        # get total amount of files
        class_files = sorted(glob.glob(f"{timeseries_folder}/{h}_{hail}/*.nc"))

        # extract years from filenames
        years_in_files = [os.path.basename(f)[:4] for f in class_files]

        n_year = np.zeros(len(years))
        for y, year in enumerate(years):

            # get number of files within this year
            n_year[y] += years_in_files.count(str(year))

        # plot bar for this year
        ax1.bar(years, n_year, -0.8, color=mwcch_plt.hail_class_colors_list[int(h)], 
                align="center", bottom=bottom)
        
        # add to bottom
        bottom += n_year

    # format x axes
    ax1.set_xticks(years, labels=years)
    ax1.set_xlim(years[0]-0.5, years[-1] + 0.5)
    # set every second x tick off
    for label in ax1.get_xticklabels()[1::2]:
        label.set_visible(False)
    
    ax1.set_xlabel("year")
    ax1.set_ylabel("# of timeseries")
    ax1.grid(False)

    # add total number of files in upper right corner of axes
    ax1.text(0.05, 0.95, f"# total: {N_total}", fontsize=12, 
                horizontalalignment='left', verticalalignment='top', 
                transform=ax1.transAxes)
    #
    plt.tight_layout()

    if output_name is not None:
        plt.savefig(output_name, bbox_inches='tight')
        plt.close()
    else:
        plt.show()
        plt.close()


# %%
if __name__ == "__main__":
    # mwcch_path = "/net/merisi/pbigalke/data/MWCC-H/netcdf"
    timeseries_path = f"/net/merisi/pbigalke/data/labelled_MSG_timeseries"

    # plot_path = "/net/merisi/pbigalke/plots/data_investigation/constructing_dataset/casestudy_20220605/timeseries"
    # if not os.path.exists(plot_path):
    #     os.makedirs(plot_path)
    # case_study = {
    #     # study settings
    #     "years": [2022], #np.arange(2006, 2024, 1)
    #     "months": [6], #np.arange(4, 10, 1)
        
    #     # MWCC-H filters
    #     "area_threshold": 30, 

    #     # time series settings
    #     "msg_res": 15,
    #     "n_frames": 4,
    #     "gap": 15,

    #     # cropping settings
    #     "cropsize": 128,
    #     "min_pix": 5,
    # }
    # settings = case_study

    plot_path = "/net/merisi/pbigalke/plots/data_investigation/constructing_dataset/prestudy_timeseries"
    if not os.path.exists(plot_path):
        os.makedirs(plot_path)
    prestudy = {
        # study settings
        "years": np.arange(2006, 2024, 1),
        "months": np.arange(4, 10, 1),
        
        # MWCC-H filters
        "area_threshold": 30, 

        # time series settings
        "msg_res": 15,
        "n_frames": 4,
        "gap": 15,

        # cropping settings
        "cropsize": 128,
        "min_pix": 5,
    }
    settings = prestudy


    timeseries_folder = clt.folder_from_study_settings(timeseries_path, settings["years"], settings["months"], 
                                                       settings["area_threshold"], settings["msg_res"], settings["n_frames"], 
                                                       settings["gap"], settings["cropsize"], settings["min_pix"])

    # plot hail class distribution
    plot_hail_class_distribution(timeseries_folder, output_name=f"{plot_path}/hail_class_distribution.png")

    # plot data distribution over years
    plot_yearly_hailclass_distribution(timeseries_folder, settings["years"], output_name=f"{plot_path}/yearly_hailclass_distribution.png")

    # channels to plot
    channels = ["IR_108", "WV_062-IR_108"]
    for channel in channels:
        # plot example timeseries
        plot_timeseries_examples_for_each_hailclass(timeseries_folder, channel, settings["n_frames"], n_examples=5, 
                                                    output_path=os.path.join(plot_path, "examples"))

# %%
