"""
Usage:

In test mode (on a sample of 100):
```
python dap_job_quality/analysis/healthcare_analysis/classify_contract_types.py
```

In production mode:
```
python dap_job_quality/analysis/healthcare_analysis/classify_contract_types.py production
```
"""

import pandas as pd
import plac
from tqdm.auto import tqdm

from dap_job_quality import logger
from dap_job_quality.utils.analysis_utils import classify_contract_type


def main(production: bool = False):
    logger.info("Loading jobs data...")
    jq_data = pd.read_parquet(
        "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_jq_dimensions.parquet"
    )

    if production:
        logger.info("Running in production mode")
        contract_data = jq_data[jq_data["subcategory"] == "CONTRACT"]
    else:
        logger.info("Running in test mode")
        contract_data = jq_data[jq_data["subcategory"] == "CONTRACT"].sample(100)

    tqdm.pandas()
    contract_data["classified_contract_type"] = contract_data[
        "sentences_split"
    ].progress_apply(classify_contract_type)

    if production:
        output_path = f"s3://open-jobs-lake/job_quality/health_social_care/health_jobs_jq_contract.parquet"
    else:
        output_path = f"s3://open-jobs-lake/job_quality/health_social_care/health_jobs_jq_contract_TEST.parquet"

    logger.info(f"Saving {len(contract_data)} records to to {output_path}")
    contract_data.to_parquet(output_path, index=False)


if __name__ == "__main__":
    plac.call(main)
