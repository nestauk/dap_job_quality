""""
This script takes the most recent salary data and runs some basic checks, to ensure the rules applied in the cleaning process are working as expected.

To run, update the salary_measures_s3 variable to the most recent salary data file, then from the root folder, run:

python -m dap_job_quality.pipeline.salary.salary_internal_tests"""


from dap_job_quality import PRINZ_BUCKET_NAME, PROJECT_DIR, logger
from dap_job_quality.getters.data_getters import load_s3_data

import pandas as pd
import unittest
import json


salary_measures_s3 = (
    "outputs/data/ojo_application/deduplicated_sample/all_salaries_data_sample.parquet"
)

ojo_sal = load_s3_data(PRINZ_BUCKET_NAME, salary_measures_s3)


class TestSalary:
    def __init__(self, df):
        self.error_dict = {}
        self.export_path = f"{PROJECT_DIR}/dap_job_quality/pipeline/salary/salary_internal_tests_log.json"
        self.df = df
        self.logger = logger

    def test_min_less_max_salaries(self):
        logger.info("Checking that min salary < max salary for all records")
        self.df["sal_diff"] = (
            self.df["max_annualised_salary"] - self.df["min_annualised_salary"]
        )
        self.df["sal_diff"].fillna(0, inplace=True)

        if self.df["sal_diff"].min() >= 0:
            "Minimum salary less than or equal to maximum salary",
            logger.info("Test passed")
        else:
            self.error_dict["min_less_max_salaries"] = self.df[self.df["sal_diff"] < 0][
                "id"
            ].to_list()
            logger.info(
                f"Test failed - list of ids where test failed has been exported to file at {self.export_path}"
            )

    def test_max_salary_500k(self):
        logger.info("Checking that all salaries are <= £500k GBP")

        if self.df["max_annualised_salary"].max() <= 500000:
            logger.info("Test passed")
        else:
            self.error_dict["max_salary_500k"] = self.df[
                self.df["max_annualised_salary"] > 500000
            ]["id"].to_list()
            logger.info(
                f"Test failed - list of ids where test failed has been exported to file at {self.export_path}"
            )

    def test_max_diff_10_times(self):
        logger.info(
            "Checking that all max salaries are less than 10 times the min salary"
        )
        if (
            self.df["max_annualised_salary"] <= self.df["min_annualised_salary"] * 10
        ).all():
            logger.info("Test passed")
        else:
            self.error_dict["max_diff_10_times"] = self.df[
                self.df["max_annualised_salary"] > self.df["min_annualised_salary"] * 10
            ]["id"].to_list()
            logger.info(
                f"Test failed - list of ids where test failed has been exported to file at {self.export_path}"
            )

    def run_all_tests(self):
        test = TestSalary(self.df)
        test.test_min_less_max_salaries()
        test.test_max_salary_500k()
        test.test_max_diff_10_times()
        with open(test.export_path, "w") as file:
            json.dump(test.error_dict, file)


if __name__ == "__main__":
    test = TestSalary(ojo_sal)
    test.run_all_tests()

    print("All tests complete")
