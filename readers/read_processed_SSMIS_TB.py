# %%
import xarray as xr
import sys
sys.path.append("..")
# import my own script
import matching_data.collect_matching_files as clct


# %%
channel_info = {
    # scene_img_1:
    "channel_8": {"scene": 'scene_img1', "number": 8, "frequency": "150+-1.2", "polarisation": "h", "intercalibrated": False, "used_for_MWCCH":True}, 
    "channel_9": {"scene": 'scene_img1', "number": 9, "frequency": "183+-6.6", "polarisation": "h", "intercalibrated": False, "used_for_MWCCH":True}, 
    "channel_10": {"scene": 'scene_img1', "number": 10, "frequency": "183+-3.0", "polarisation": "h", "intercalibrated": False, "used_for_MWCCH":True}, 
    "channel_11": {"scene": 'scene_img1', "number": 11, "frequency": "183+-1.0", "polarisation": "h", "intercalibrated": False, "used_for_MWCCH":True}, 
    # scene_img_2:
    "channel_17": {"scene": 'scene_img2', "number": 17, "frequency": "91+-0.9", "polarisation": "v", "intercalibrated": True, "used_for_MWCCH":True}, 
    "channel_18": {"scene": 'scene_img2', "number": 18, "frequency": "91+-0.9", "polarisation": "h", "intercalibrated": True, "used_for_MWCCH":True}, 
    "channel_25": {"scene": 'scene_img2', "number": 25, "frequency": "85", "polarisation": "v", "intercalibrated": False, "used_for_MWCCH":False}, 
    "channel_26": {"scene": 'scene_img2', "number": 26, "frequency": "85", "polarisation": "h", "intercalibrated": False, "used_for_MWCCH":False}, 
}

def _get_scenes(channels):
    scenes = []
    for ch in channels:
        scene = channel_info[ch]["scene"]
        if scene not in scenes:
            scenes.append(scene)
    return scenes


# %%
def read(file_path):
    """ read processed CMSAF SSMIS brightness temp
    """
    with xr.open_dataset(file_path) as dataset:
        return dataset

def get_y_m_d_from_filepath(file_path):
    
    return None


# %%
if __name__ == '__main__':

    # test on example data    
    path = "/net/merisi/pbigalke/data/CMSAF_SSMIS_processed"
    years = [2022]
    months = [6]
    days = [5]
    all_files = clct.get_files_in_study_period(path, years, months, days)
    for f in all_files:
        print(f)
        print(read(f))

# %%
