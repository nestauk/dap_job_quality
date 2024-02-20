""""
This script takes the most recent salary data and runs some basic checks, to ensure the rules applied in the cleaning process are working as expected.

To run, update the salary_measures_s3 variable to the most recent salary data file, then from the root folder, run:

python -m dap_job_quality.pipeline.salary_internal_tests"""


from dap_job_quality import PRINZ_BUCKET_NAME
from dap_job_quality.getters.data_getters import load_s3_data, load_s3_excel, save_to_s3

import pandas as pd
import unittest


salary_measures_s3 = (
    "outputs/data/ojo_application/deduplicated_sample/all_salaries_data_sample.parquet"
)

ojo_sal = load_s3_data(PRINZ_BUCKET_NAME, salary_measures_s3)


class InternalSalaryCheck(unittest.TestCase):
    """Checks if the cleaning rules have been properly applied"""

    def test_min_less_max_salaries(self):
        ojo_sal["sal_diff"] = (
            ojo_sal["max_annualised_salary"] - ojo_sal["min_annualised_salary"]
        )
        ojo_sal["sal_diff"].fillna(0, inplace=True)
        self.assertTrue(
            ojo_sal["sal_diff"].min() >= 0,
            "Minimum salary less than or equal to maximum salary",
        )

    def test_max_diff(self):
        self.assertTrue(
            (
                ojo_sal["max_annualised_salary"]
                <= ojo_sal["min_annualised_salary"] * 10
            ).all(),
            "Maximum salary is less than 10 times the minimum salary",
        )

    def test_max_salary(self):
        self.assertTrue(
            ojo_sal["max_annualised_salary"].max() <= 500000,
            "Maximum salary is less than %",
        )


if __name__ == "__main__":
    unittest.main(module=__name__, argv=[""], exit=False)
