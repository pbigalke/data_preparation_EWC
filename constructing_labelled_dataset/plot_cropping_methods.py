# %%
import numpy as np
import os
import sys
sys.path.append('..')
import readers.read_MSG as msg_read
from config. domain_info import domain_expats, domain_expats_hail
import readers.read_processed_MWCC_H as mwcch_read
import matching_data.collect_matching_files as match
import helpers.datetime_helper as hlp
import constructing_dataset.crop_over_hail_or_overpass as cropover
import plotting.plot_MWCC_H as mwcc_plt

c_hail_area = "cyan"
c_over = "lime"
c_diffWVIR = "gold"
c_ot = "red"

# %%
def plot_different_crop_positions(msg_timestamp_data, mwcch_data, cropsize, min_pixel, output_path, 
                                  crops=["maxhailarea"], recenter=False, mwcch_mode="hail_class", 
                                  domain=domain_expats_hail):

    # get area coverage
    overpass_area = mwcch_read.area_percentage_covered_by_overpass(mwcch_data.hail_class.values)

    mark_points=[]
    draw_subdomains=[]

    if crops is not None:
        for crop in crops:
            if crop == "maxhailarea":
                try:
                    # get crop extent over ------------------------------------------------------------------- max hail area
                    cg_lon, cg_lat, minlon, maxlon, minlat, maxlat = \
                        cropover.get_crop_extent_over_maxhailarea(mwcch_data, cropsize, min_pixel=min_pixel)
                except TypeError:
                    print("ERROR: crop extent could not be calculated.")
                    continue

                mark_points.append([cg_lon, cg_lat, c_hail_area, 'x'])
                draw_subdomains.append([minlon, maxlon, minlat, maxlat, c_hail_area, '-'])

                if recenter:
                    # get crop extent over ------------------------------------------------------------------- diff WV-IR within crop
                    cg_lon_diff, cg_lat_diff, minlon_diff, maxlon_diff, minlat_diff, maxlat_diff = \
                        cropover.recenter_crop_over_highest_clouds(msg_timestamp_data, [minlon, maxlon, minlat, maxlat])

                    # get crop extent over ------------------------------------------------------------------- OT area within crop
                    cg_lon_OT, cg_lat_OT, minlon_OT, maxlon_OT, minlat_OT, maxlat_OT = \
                        cropover.recenter_crop_over_highest_clouds(msg_timestamp_data, [minlon, maxlon, minlat, maxlat], mode='OT')

                    mark_points.extend([[cg_lon_diff, cg_lat_diff, c_diffWVIR, 'x'], 
                                        [cg_lon_OT, cg_lat_OT, c_ot, 'x']])
                    draw_subdomains.extend([[minlon_diff, maxlon_diff, minlat_diff, maxlat_diff, c_diffWVIR, '-'],
                                            [minlon_OT, maxlon_OT, minlat_OT, maxlat_OT, c_ot, '-']])

                    
            elif crop == "overpassarea":
                # get crop extent over ------------------------------------------------------------------- overpass area
                cg_lon_over, cg_lat_over, minlon_over, maxlon_over, minlat_over, maxlat_over = \
                    cropover.get_crop_extent_over_overpassarea(mwcch_data, cropsize)
                mark_points.append([cg_lon_over, cg_lat_over, c_over, 'x'])
                draw_subdomains.append([minlon_over, maxlon_over, minlat_over, maxlat_over, c_over, '-'])


    # ---------------------------------------------------------------------
    # loop over channels
    for channel in channels:

        # get data from channel
        if "-" in channel:
            chan1 = channel.split("-")[0]
            chan2 = channel.split("-")[1]
            print(chan1, chan2)
            msg_tb = msg_timestamp_data[chan1] - msg_timestamp_data[chan2]
        else:
            msg_tb = msg_timestamp_data[channel]


        # --------------------------------------------------------------------- plot MSG, MWCCH and crops
        # plot last timestamp with hail area crop
        dt = hlp.get_datetimestring_from_npdatetime(msg_timestamp_data.time.values)
        title_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:8]} {dt[-4:-2]}:{dt[-2:]}, coverage: {overpass_area} %"
        outname = f"{output_path}/{dt}_{channel}_{mwcch_mode}_{domain[0]}-{domain[1]}_{domain[2]}-{domain[3]}"  
        if crops is not None: 
            outname += "_crop_over" + "".join([f"_{crop}" for crop in crops])
            if recenter:
                outname += "_recentered"

        # plot crops over MSG and MWCC-H
        mwcc_plt.plot_mwcch_over_MSG(msg_timestamp_data.lon.values, msg_timestamp_data.lat.values, msg_tb.values, channel, 
                                    mwcch_data.lon.values, mwcch_data.lat.values, mwcch_data.POH.values, mwcch_mode=mwcch_mode,
                                    mark_points=mark_points, draw_subdomains=draw_subdomains, 
                                    vmin=channels[channel]["vmin"], vmax=channels[channel]["vmax"], 
                                    domain=domain, title=title_str, path_out=f"{outname}.png")




# %%
if __name__ == "__main__":
    # mwcch_path = "/net/merisi/pbigalke/data/MWCC-H/netcdf"
    msg_path = msg_read.MSG_PATH
    mwcch_path = mwcch_read.MWCCH_MSGGRID_PATH

    output_path = "/net/merisi/pbigalke/plots/data_investigation/constructing_dataset/crop_positions"
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    years = [2022]
    months = [6]
    days = [5, 20]
    msg_res = 15
    cropsize = 128
    min_pixel = 1
    channels = {#'WV_062-IR_108': {"vmin":-60, "vmax":5}, 
                'IR_108': {"vmin":200, "vmax":280}, 
    }
    # extract unique channels that should be read in
    needed_channels = [ch.split('-') for ch in channels.keys()]
    unique_channels = list(set(item for sublist in needed_channels for item in sublist))

    # ---------------------------------------------------------------------
    # load all mwcc-h files in study period
    mwcch_files = match.get_files_in_study_period(mwcch_path, years, months=months, days=days)
    print(len(mwcch_files))

    # loop over mwcch files
    for file in mwcch_files:
        print()
        print(os.path.basename(file))

        # ---------------------------------------------------------------------
        # read in mwcc_file
        mwcch_data = mwcch_read.read(file)

        # get end time of overpass
        mwcch_end = mwcch_data.end_scan

        # get corresponding MSG timestamp and file
        msg_timestamp = match.get_closest_MSG_timestamps(mwcch_end, which="following", msg_res=msg_res)
        msg_file = msg_read.get_MSG_files_from_timestamps(msg_timestamp)[0]

        # read in MSG data and filter for timestamp
        msg_timestamp_data = msg_read.read(msg_file, channels=unique_channels).sel(time=msg_timestamp)

        for domain in [domain_expats_hail, domain_expats]:
            
            # plot_different_crop_positions(msg_path, mwcch_path, output_path, years, months, days, msg_res)
            plot_different_crop_positions(msg_timestamp_data, mwcch_data, cropsize, min_pixel, output_path, 
                                        domain=domain, crops=["maxhailarea", "overpassarea"], mwcch_mode="hail_class") # crops=["maxhailarea", "overpassarea"]
            plot_different_crop_positions(msg_timestamp_data, mwcch_data, cropsize, min_pixel, output_path, 
                                        domain=domain, crops=None, mwcch_mode="hail_class") # crops=["maxhailarea", "overpassarea"]
# %%
