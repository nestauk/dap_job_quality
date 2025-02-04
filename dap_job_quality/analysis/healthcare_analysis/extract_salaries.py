import os

os.system(
    f"pip install --no-warn-conflicts --disable-pip-version-check -qr {os.path.dirname(os.path.realpath(__file__))}/metaflow_requirements.txt 1> /dev/null"  # nosec
)

from metaflow import FlowSpec, step, Parameter, batch
import pandas as pd

from dap_job_quality import BUCKET_NAME, logging
from dap_job_quality.getters.data_getters import (
    save_to_s3,
    load_s3_json,
    get_s3_data_paths,
)

METAFLOW_PATH = "metaflow/data/ReedAdCurateFlow/1700394027436526/"


def process_data(s3_key, afs_ids):
    data = load_s3_json(BUCKET_NAME, s3_key)
    temp_df = pd.DataFrame(data)
    temp_df["id"] = pd.to_numeric(temp_df["id"], downcast="integer")
    temp_df = temp_df[temp_df["id"].isin(afs_ids)]
    print(f"Number of ads retrieved from {s3_key}: \n {len(temp_df)}")
    print(len(temp_df))
    temp_df["raw_salary_float"] = temp_df["raw_salary"].astype(float)
    temp_df["raw_min_salary_float"] = temp_df["raw_min_salary"].astype(float)
    temp_df["raw_max_salary_float"] = temp_df["raw_max_salary"].astype(float)
    return temp_df


METAFLOW_PATH = "metaflow/data/ReedAdCurateFlow/1700394027436526/"


class ProcessSalariesFlow(FlowSpec):

    production = Parameter("production", default=False, help="Run in production mode")
    start_index = Parameter(
        "start_index", default=0, help="Starting index for processing"
    )

    @step
    def start(self):
        """Load in-scope job IDs and fetch S3 file paths."""
        print(
            f"Starting flow with production={self.production} and start_index={self.start_index}"
        )

        self.in_scope_ids = pd.read_pickle(
            "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_ids.pkl"
        )

        self.s3_keys = get_s3_data_paths(
            BUCKET_NAME, root=METAFLOW_PATH, file_types=["*.json"]
        )

        if not self.production:
            self.s3_keys = self.s3_keys[:10]  # Process only a subset in test mode

        self.next(self.process_data)

    @batch(cpu=4, memory=16000)
    @step
    def process_data(self):
        """Process each JSON file and extract salary metadata."""
        from tqdm import tqdm

        from dap_job_quality import BUCKET_NAME, logging
        from dap_job_quality.getters.data_getters import (
            save_to_s3,
            load_s3_json,
            get_s3_data_paths,
        )

        master_data_size = 0
        for i, key in tqdm(enumerate(self.s3_keys[self.start_index :])):
            temp_df = process_data(key, self.in_scope_ids)
            master_data_size += len(temp_df)

            save_to_s3(
                BUCKET_NAME,
                temp_df,
                f"job_quality/health_social_care/salaries/metadata_interim_production_{self.production}/salaries_chunk_{i+self.start_index}.parquet",
            )

        paths = get_s3_data_paths(
            BUCKET_NAME,
            root=f"job_quality/health_social_care/salaries/metadata_interim_production_{self.production}",
            file_types=["*.parquet"],
        )
        paths = ["s3://" + BUCKET_NAME + "/" + path for path in paths]
        master_df = pd.concat(pd.read_parquet(f) for f in paths)

        if master_data_size != len(master_df):
            print(
                f"Data size mismatch. Should be {master_data_size} but is actually {len(master_df)}"
            )
        else:
            print(f"Final data comprises {len(master_df)} job ads")

        save_to_s3(
            BUCKET_NAME,
            master_df,
            f"job_quality/health_social_care/salaries/salary_metadata_production_{self.production}.parquet",
        )
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        print("Processing complete.")


if __name__ == "__main__":
    ProcessSalariesFlow()
