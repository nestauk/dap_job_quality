"""
Usage:

```
python dap_job_quality/analysis/healthcare_analysis/extract_job_quality.py --package-suffixes=.txt,.py,.yaml --datastore=s3 run --max-num-splits=150
```
"""
import os

os.system(
    f"pip install --no-warn-conflicts --disable-pip-version-check -qr {os.path.dirname(os.path.realpath(__file__))}/metaflow_requirements.txt 1> /dev/null"  # nosec
)

from metaflow import FlowSpec, step, Parameter, S3, batch
import pandas as pd


class JobQualityFlow(FlowSpec):
    batch_size = Parameter("batch_size", default=1000, help="Number of rows per batch")

    @step
    def start(self):
        """Load the job quality pipeline and input data locally."""
        print("Flow starting...")
        self.next(self.load_data)

    @batch(
        cpu=8,
        memory=32000,
    )
    @step
    def load_data(self):
        # import toolz

        # Load the job descriptions and filter for in-scope IDs
        print("Loading job descriptions...")

        job_descriptions = pd.read_parquet(
            "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_descriptions.parquet"
        )

        in_scope_ids = pd.read_pickle(
            "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_ids.pkl"
        )

        print("Filtering job descriptions with health SOC codes...")
        self.health_job_descriptions = job_descriptions[
            job_descriptions["id"].isin(in_scope_ids)
        ]
        print(self.health_job_descriptions.head())
        print(f"Number of job adverts in total: {len(self.health_job_descriptions)}")

        # Split the data into batches
        print("Splitting data into batches...")
        # self.batches = list(enumerate(toolz.partition_all(self.batch_size, self.health_job_descriptions)))
        self.batches = list(
            enumerate(
                [
                    self.health_job_descriptions.iloc[i : i + self.batch_size]
                    for i in range(
                        0, len(self.health_job_descriptions), self.batch_size
                    )
                ]
            )
        )
        print(f"Total batches: {len(self.batches)}")
        print(self.batches[0])

        self.next(self.process_batches, foreach="batches")

    @batch(
        cpu=16,
        memory=64000,
    )
    @step
    def process_batches(self):
        """Process each batch to extract job quality."""
        from dap_job_quality.pipeline.find_job_quality import JobQuality

        self.job_quality = JobQuality()
        self.job_quality.load()

        batch_index, batch = self.input
        print(f"Processing batch {batch_index} with {len(batch)} rows")
        (
            self.jq_df_filtered,
            self.job_id_to_target_phrase,
        ) = self.job_quality.extract_job_quality(
            batch,
            id_col="id",
            text_col="description",
        )
        self.jq_df_filtered_path = f"s3://open-jobs-lake/job_quality/health_social_care/jq_dimensions_{batch_index}.parquet"
        self.jq_df_filtered.to_parquet(self.jq_df_filtered_path, index=False)

        self.next(self.join)

    @batch(
        cpu=8,
        memory=32000,
    )
    @step
    def join(self, inputs):
        """Merge all batch results into a single output."""
        # Collect all batch results
        self.jq_results = pd.concat(
            [pd.read_parquet(inp.jq_df_filtered_path) for inp in inputs],
            ignore_index=True,
        )

        self.next(self.save_results)

    @batch(
        cpu=8,
        memory=32000,
        use_tmpfs=True,
    )
    @step
    def save_results(self):
        """Save the final results to S3."""
        output_path = "s3://open-jobs-lake/job_quality/health_social_care/health_jobs_jq_dimensions.parquet"
        self.jq_results.to_parquet(output_path, index=False)
        print(f"Saved final results to {output_path}")

        self.next(self.end)

    @step
    def end(self):
        """End of the flow."""
        print("Job Quality Flow completed successfully!")


if __name__ == "__main__":
    JobQualityFlow()
