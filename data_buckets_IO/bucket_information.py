# Script to save all bucket names and their respective file path prefixes

def get_all_bucket_names():
    """Get all bucket names used in the project
    :return: List of bucket names
    """
    return [
        'expats-random-msg-timeseries-100pix-8frames',
        'mwcch-hail-regrid-msg',
        'expats-msg-training'
    ]

def get_bucket_prefix(bucket_name, year, month, day):
    """Get the prefix for the given bucket and date
    :param bucket_name: Name of the S3 bucket
    :param year: Year of the data
    :param month: Month of the data
    :param day: Day of the data
    :return: Prefix for the S3 objects
    """
    if bucket_name == 'expats-random-msg-timeseries-100pix-8frames':
        return f"output/data/timeseries_crops/{year:04d}/{month:02d}/{day:02d}/MSG_timeseries_{year:04d}-{month:02d}-{day:02d}_"
    
    elif bucket_name == 'mwcch-hail-regrid-msg':
        return f"{year:04d}/{month:02d}/{day:02d}/{year:04d}{month:02d}{day:02d}_"
    
    elif bucket_name == 'expats-msg-training':
        path_dir = "/data/sat/msg/ml_train_crops/IR_108-WV_062-CMA_FULL_EXPATS_DOMAIN"
        return f"{path_dir}/{year:04d}/{month:02d}/merged_MSG_CMSAF_{year:04d}-{month:02d}-{day:02d}"
    
    return None