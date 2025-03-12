import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import plotly.io as pio

pio.renderers.default = "notebook"

from dap_job_quality.getters.healthcare import *
from dap_job_quality.analysis.healthcare_analysis.utils import (
    create_uk_heatmap,
    plot_years,
    grouped_bar,
    stacked_bar,
    get_groups_w_valid_sample_size,
)

if __name__ == "__main__":
    healthcare_sample = get_salaries_w_soc_region()
    job_titles = get_occupations()

    healthcare_sample["year"] = pd.to_datetime(
        healthcare_sample["created_x"], dayfirst=True
    ).dt.year

    # Not much data from 2020 so get rid of that year
    healthcare_sample = healthcare_sample[healthcare_sample["year"] > 2020]

    # Lots of ads for 'SEN TA' which I believe is out of scope, so get rid of these
    healthcare_sample = healthcare_sample[
        healthcare_sample["job_title_raw_x"] != "SEN TA"
    ]

    in_scope_ids = healthcare_sample["id"].unique()

    job_qual = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_jq_dimensions.parquet"
    )
    job_qual = job_qual[job_qual["id"].isin(in_scope_ids)]

    job_qual_processed, dimensions_wide = analysis_utils.process_jq_data(job_qual)

    sample_w_salaries_dimensions = pd.merge(
        healthcare_sample, dimensions_wide, on="id", how="left"
    )

    contracts_full = get_contracts_full()
    contract_df = get_contract_per_job()
    contracts_complete = get_contracts_complete()

    # inner join to make sure we don't include 2020 data; and that we are only including jobs that have contract info
    contracts_salaries_soc_region = pd.merge(
        contracts_complete, healthcare_sample, on="id", how="inner"
    )

    healthcare_sample.to_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/outputs/healthcare_sample.parquet"
    )

    sample_w_salaries_dimensions.to_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/outputs/sample_w_salaries_dimensions.parquet"
    )

    contracts_salaries_soc_region.to_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/outputs/contracts_salaries_soc_region.parquet"
    )
