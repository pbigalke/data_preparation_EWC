# This module provides functions to determine MSG crop extents over hail areas or overpass areas from MWCCH data.
# 
# %%
import numpy as np
import sys
sys.path.append('..')
import readers.read_processed_MWCCH as mwcch_read

# %%
# main method to get crop extent over maximum hail area
def get_crop_extent_over_maxhailarea(mwcch_data, cropsize, min_pixel=1):
    """
    Get the extent of a crop centered over the area with the maximum hail class in the MWCCH data. 
    If no hail is present, this will result in a crop centered over the overpass area.

    :param mwcch_data: MWCCH xarray dataset
    :param cropsize: Size of the square crop in number of pixels
    :param min_pixel: Minimum number of pixels in maximum hail class to be considered valid
    :return: Tuple of (cg_lon, cg_lat, lon_min, lon_max, lat_min, lat_max) defining the crop center and extent
    """
    ###### does only work for MSG-regridded MWCC-H data ######
    
    # get max hail class in mwcch data
    max_hail_class_number = mwcch_read.get_max_hail_class(mwcch_data.hail_class.values, min_pixel=min_pixel)
    
    # mask mwcc-h data where maximum hail class occurs
    masked_data = mwcch_data.where(mwcch_data.hail_class == max_hail_class_number, drop=True)

    # set all hail class values to 1 where is not NaN (to make sure that the center of mass is calculated correctly)
    masked_data['hail_class'] = masked_data.hail_class.where(np.isnan(masked_data.hail_class), 1)

    # calculate center of mass for variable
    cg_lon, cg_lat = get_center_of_mass_for_variable(masked_data.lon, masked_data.lat, masked_data.hail_class.values)

    # get extent of crop over hail area
    minlon, maxlon, minlat, maxlat = get_crop_extent_from_center_choords(mwcch_data.lon.values, mwcch_data.lat.values, 
                                                                         cg_lon, cg_lat, cropsize)

    return cg_lon, cg_lat, minlon, maxlon, minlat, maxlat

# %%
# some helper functions used to calculate crop extent over hail area
def get_center_of_mass_for_variable(msg_lon, msg_lat, variable):
    """
    Calculates the center of mass for a given variable over a 2D grid defined by longitude and latitude arrays.

    :param msg_lon: Array of longitudes
    :param msg_lat: Array of latitudes
    :param variable: 2D array of variable values corresponding to the lon/lat grid
    """
    # Create 2D longitude and latitude arrays using meshgrid
    lon2d, lat2d = np.meshgrid(msg_lon, msg_lat)
    
    # Create a mask to filter out NaN values
    mask = ~np.isnan(variable)
    
    # Apply the mask to the 2D latitude, longitude, and hail class values
    filtered_lon = lon2d[mask]
    filtered_lat = lat2d[mask]
    filtered_var = variable[mask]
    
    # Calculate the center of mass excluding NaN values
    cg_lat = np.sum(filtered_lat * filtered_var) / np.sum(filtered_var)
    cg_lon = np.sum(filtered_lon * filtered_var) / np.sum(filtered_var)

    return cg_lon, cg_lat

def get_closest_index(arr, val):
    """
    Find the index of the closest value in a sorted array to a given value.
        
    :param arr: Array of sorted values, e.g., latitudes or longitudes
    :param val: Value to find the closest index for
    :return: Index of the closest value in the array
    """
    idx = np.searchsorted(arr, val)
    # clip to last index
    if idx == len(arr):
        idx = -1
    # find closest neighbors
    if (val - arr[idx-1]) <= (arr[idx] - val):
        idx -= 1
    return idx

def shift_away_from_data_edge(idx, data_dim, padding):
    """
    Ensure that a crop centered at a given index fits within the data boundaries 
    by shifting the crop inwards away from the data edge if necessary.

    :param idx: Index of the center point
    :param data_dim: Total size of the data dimension
    :param padding: Padding size to ensure the crop fits within the data boundaries

    :return: Adjusted index of the center point
    """
    # shift center point away from domain borders to fit whole crop
    if idx < padding:
        idx = int(padding)
    elif idx > (data_dim-1 - padding):
        idx = int(data_dim-1 - padding)
    return idx

def get_crop_extent_from_center_choords(msg_lons, msg_lats, loc_lon, loc_lat, cropsize):
    """
    Find the extent of a square crop centered at given coordinates, ensuring it fits within the data boundaries.

    :param msg_lons: Array of longitudes
    :param msg_lats: Array of latitudes
    :param loc_lon: Center longitude of the crop
    :param loc_lat: Center latitude of the crop
    :param cropsize: Size of the square crop in number of pixels

    :return: Tuple of (lon_min, lon_max, lat_min, lat_max) defining the crop extent
    """
    # find center of crop
    idx_lon_c = get_closest_index(msg_lons, loc_lon)
    idx_lat_c = get_closest_index(msg_lats, loc_lat)

    # shift center position away from edge to fit crop into domain
    idx_lon_c= shift_away_from_data_edge(idx_lon_c, len(msg_lons), cropsize/2.)
    idx_lat_c = shift_away_from_data_edge(idx_lat_c, len(msg_lats), cropsize/2.)

    # get indices of edges of crop
    idx_lon_min = idx_lon_c - int(cropsize/2.)
    idx_lon_max = idx_lon_min + int(cropsize) - 1
    # need to substract 1 as xr.dataset.sel(lon=slice(minlon, maxlon)) includes the edges
    idx_lat_min = idx_lat_c - int(cropsize/2.)
    idx_lat_max = idx_lat_min + int(cropsize) - 1

    # get corresponding lon lat extent
    lon_min = msg_lons[idx_lon_min]
    lon_max = msg_lons[idx_lon_max]
    lat_min = msg_lats[idx_lat_min]
    lat_max = msg_lats[idx_lat_max]

    return lon_min, lon_max, lat_min, lat_max

# %%
# an additional method to recenter an already existing crop over the highest clouds within that crop
def recenter_crop_over_highest_clouds(msg_timestamp_data, crop_extent, mode="all"):
    """
    Recenters a crop over the highest clouds within the given crop extent based on the difference between the WV 6.2 µm and IR 10.8 µm channels.

    :param msg_timestamp_data: xarray dataset for a single MSG timestamp containing WV 6.2 µm and IR 10.8 µm channels
    :param crop_extent: Tuple of (lon_min, lon_max, lat_min, lat_max) defining the current crop extent
    :param mode: Mode for recentering. "all" considers all difference values, "OT" considers only positive difference values (overshooting tops).
    :return: Tuple of (cg_lon_recentered, cg_lat_recentered, minlon, maxlon, minlat, maxlat) defining the new crop center and extent
    """
    # get difference between 6.2 and 10.8 channels
    diffWVIR = msg_timestamp_data.WV_062 - msg_timestamp_data.IR_108

    # only look at values within original crop over hail area
    diffWVIR_in_crop = diffWVIR.sel(lon=slice(crop_extent[0], crop_extent[1]), lat=slice(crop_extent[2], crop_extent[3]))

    # get cropsize
    cropsize = len(diffWVIR_in_crop.lon.values)

    # if looking only at OT proxy
    if mode == "OT":
        # consider only positive difference values (=OT)
        diffWVIR_in_crop = diffWVIR_in_crop.where(diffWVIR > 0)

    # find center of mass for diff-WV-IR values WITHIN FIRST CROP OVER HAIL AREA
    cg_lon_recentered = np.sum(diffWVIR_in_crop.lon * diffWVIR_in_crop) / np.sum(diffWVIR_in_crop)
    cg_lat_recentered = np.sum(diffWVIR_in_crop.lat * diffWVIR_in_crop) / np.sum(diffWVIR_in_crop)

    # overwrite hail area crop with new recentered crop extent
    minlon, maxlon, minlat, maxlat = get_crop_extent_from_center_choords(msg_timestamp_data.lon.values, msg_timestamp_data.lat.values, 
                                                                            cg_lon_recentered, cg_lat_recentered, cropsize)
    
    return cg_lon_recentered.values, cg_lat_recentered.values, minlon, maxlon, minlat, maxlat

