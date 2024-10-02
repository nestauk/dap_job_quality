from dap_job_quality import BUCKET_NAME
from dap_job_quality.getters.data_getters import load_s3_data


def get_stratified_sample():
    return load_s3_data(
        BUCKET_NAME,
        "job_quality/prodigy/labelling_data/ojo_sample_stratified_ts_956_seed42_20240509.csv",
    )
