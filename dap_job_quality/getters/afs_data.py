from dap_job_quality.getters.data_getters import load_s3_data
from dap_job_quality import BUCKET_NAME


def get_eyp_ads():
    return load_s3_data(BUCKET_NAME, "job_quality/early_years/eyp_job_ads.parquet")


def get_sim_occ_ads():
    return load_s3_data(BUCKET_NAME, "job_quality/early_years/sim_occs_job_ads.parquet")
