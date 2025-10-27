"""
reading routines for radar data from DWD

"""
import xarray as xr

def read_radar_DWD(path_radolan_DE, day):
    """
    function to read the radar files of a given day
    Args:
        path_radolan_DE (_type_): _description_
    """
    yy = day[0:4]
    mm = day[4:6]
    filename = path_radolan_DE+yy+'/'+mm+'/YW_2017.002_'+day+'.nc'
    data = xr.open_dataset(filename)    
    
    return(data)

