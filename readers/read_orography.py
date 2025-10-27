"""
reading routines for orography

"""

import xarray as xr
import sys
sys.path.append('..')
from config.data_file_dirs import orography_file
  

def read_orography():
    
    data = xr.open_dataset(orography_file)
    return data

