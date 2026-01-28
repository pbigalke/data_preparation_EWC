"""
Define your study settings here.
"""
import numpy as np

# --------------------------- study period --------------------------- #
# years to be included in the study, available are years between 2006 and 2023
years = [2023]  # np.arange(2006, 2024, 1)#
# months to be included in the study, available are months between 4 and 9
months = [7]  # np.arange(4, 10, 1)
# days to be included in the study, useful either single days or all days in month
days = [24]  # np.arange(1, 32, 1)

# ---------------------------- MWCC-H filters ---------------------------- #
# minimum area covered by overpass in percent of whole EXPATS domain
area_threshold = 30  
# minimum number of pixels in maximum hail class for qualifying as hail class label
min_pix = 5

# ---------------------------- time series settings ---------------------------- #
# MSG resolution in minutes
msg_res = 15  
# MSG channel(s) to be used
msg_channels = ["IR_108"]  
# number of frames in timeseries
n_frames = 4 
# Gap between subsequent timeseries in minutes (if negative results in overlapping timeseries)
gap = 15

# ---------------------------- cropping settings ---------------------------- #
# size of square crop in pixels
cropsize = 128  

# ---------------------------- output settings ---------------------------- #
# output path for labelled dataset
out_path = "/data/labelled_datasets"
