import pytest
import pandas as pd
from dap_job_quality.pipeline.find_job_quality import JobQuality


@pytest.fixture
def sample_job_adverts():
    """
    Fixture to provide a sample DataFrame of job adverts for testing.
    """
    return pd.DataFrame(
        [
            {
                "id": 123,
                "description": "This is a job adverts. It has many benefits such as a pension scheme and a cycle to work scheme.",
            },
            {
                "id": 234,
                "description": "This is a job adverts for a job at a bank. There are free childcare vouchers. We also offer a yearly bonus and generous salary.",
            },
        ]
    )


@pytest.fixture
def job_quality_model():
    """
    Fixture to initialize and load the JobQuality model.
    """
    job_quality = JobQuality()
    job_quality.load()
    return job_quality


def test_extract_job_quality(job_quality_model, sample_job_adverts):
    """
    Unit test to check if extract_job_quality extracts the correct target phrases.
    """
    jq_df_filtered, job_id_to_target_phrase = job_quality_model.extract_job_quality(
        sample_job_adverts,
        id_col="id",
        text_col="description",
    )

    # Expected result
    expected_output = {
        123: ["Cycle to work", "benefits", "pension", "pension scheme"],
        234: ["childcare vouchers", "compensation", "performance bonus"],
    }

    # Assertions to check if the output is as expected
    assert isinstance(jq_df_filtered, pd.DataFrame), "Output should be a DataFrame"
    assert isinstance(
        job_id_to_target_phrase, dict
    ), "job_id_to_target_phrase should be a dictionary"
    assert (
        job_id_to_target_phrase == expected_output
    ), "Extracted target phrases do not match expected output"
